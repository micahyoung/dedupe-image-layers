package main

import (
	"archive/tar"
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/daemon"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"io"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	sourceImageName := flag.String("source-ref", "", "image ref")
	destinationImageName := flag.String("destination-ref", "", "image ref")
	remote := flag.Bool("remote", false, "remote to registry")
	flag.Parse()

	if *sourceImageName == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(*sourceImageName, *destinationImageName, *remote); err != nil {
		log.Fatal(err)
	}
}

func run(sourceImageName, destinationImageName string, isRemote bool) error {
	var sourceImage v1.Image
	sourceRef, err := name.ParseReference(sourceImageName, name.WeakValidation)
	if err != nil {
		return err
	}
	destinationTag, err := name.NewTag(destinationImageName)
	if err != nil {
		return err
	}

	if isRemote {
		fmt.Printf("loading remote image %s\n", sourceImageName)
		sourceImage, err = remote.Image(sourceRef, remote.WithAuthFromKeychain(authn.DefaultKeychain))
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("loading daemon image %s\n", sourceImageName)
		sourceImage, err = daemon.Image(sourceRef, daemon.WithUnbufferedOpener())
		if err != nil {
			return err
		}
	}

	fmt.Println("loading manifest")
	sourceManifest, err := sourceImage.Manifest()
	if err != nil {
		return err
	}

	fmt.Println("loading config")
	sourceConfigFile, err := sourceImage.ConfigFile()
	if err != nil {
		return err
	}
	sourceConfigFile.DeepCopy()

	var sourceLayerHistory []v1.History
	for _, h := range sourceConfigFile.History {
		if h.EmptyLayer {
			continue
		}
		sourceLayerHistory = append(sourceLayerHistory, h)
	}
	sourceDiffIDs := sourceConfigFile.RootFS.DiffIDs

	fmt.Println("remove existing layers and history")
	sourceConfigFile.History = nil
	sourceConfigFile.RootFS.DiffIDs = nil

	fmt.Println("creating destination image")
	destinationImage, err := mutate.ConfigFile(empty.Image, sourceConfigFile)
	if err != nil {
		return err
	}

	fmt.Println("filtering layers")
	for i, sourceDiffID := range sourceDiffIDs {
		fmt.Printf("reading layer %d\n", i)

		sourceLayer, err := sourceImage.LayerByDiffID(sourceDiffID)
		if err != nil {
			return err
		}

		sourceLayerType, err := sourceLayer.MediaType()
		if err != nil {
			return err
		}

		var layer v1.Layer
		switch sourceLayerType {
		case types.DockerForeignLayer:
			layer = sourceLayer
		default:
			fmt.Println("filtering layer")
			layer, err = tarball.LayerFromOpener(filteredLayer(sourceLayer), tarball.WithCompressionLevel(9), tarball.WithCompressedCaching)
			if err != nil {
				return err
			}
		}

		fmt.Println("appending layer")
		fmt.Printf("History: %s\n", sourceLayerHistory[i])
		destinationImage, err = mutate.Append(destinationImage, mutate.Addendum{
			Layer:       layer,
			MediaType:   sourceLayerType,
			History:     sourceLayerHistory[i],
			URLs:        sourceManifest.Layers[i].URLs,
			Annotations: sourceManifest.Layers[i].Annotations,
		})
	}

	if isRemote {
		fmt.Printf("writing remote image %s\n", destinationImageName)
		if err := remote.Write(destinationTag, destinationImage, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
			return err
		}
	} else {
		fmt.Printf("writing daemon image %s\n", destinationImageName)
		output, err := daemon.Write(destinationTag, destinationImage)
		if err != nil {
			return err
		}
		io.Copy(os.Stdout, bytes.NewBuffer([]byte(output)))
	}

	return nil
}

func filteredLayer(originalLayer v1.Layer) tarball.Opener {
	return func() (io.ReadCloser, error) {
		pipeReader, pipeWriter := io.Pipe()
		fmt.Println("loading layer")
		layerReader, err := originalLayer.Uncompressed()
		if err != nil {
			return nil, err
		}
		fmt.Println("...done")

		tarReader := tar.NewReader(layerReader)

		go func() {
			fmt.Println("filtering layer")
			tarWriter := tar.NewWriter(pipeWriter)

			hardLinkSources := map[string]string{}
			var savedSpace int64
			i := 0
			for {
				i++
				tarHeader, err := tarReader.Next()

				if err == io.EOF {
					fmt.Printf("done filtering (saved %d bytes)\n", savedSpace)

					if err := tarWriter.Close(); err != nil {
						panic(err)
					}
					if err := pipeWriter.CloseWithError(io.EOF); err != nil {
						panic(err)
					}

					return
				}
				if err != nil {
					panic(err)
					//	if err := pipeWriter.CloseWithError(err); err != nil {
					//		panic(err)
					//	}
					//	return
				}

				isHardLinkCandidate := tarHeader.Typeflag == tar.TypeReg && tarHeader.Size > 10000

				// copy normally if non-trivially-sized regular file
				if !isHardLinkCandidate {
					if err := tarWriter.WriteHeader(tarHeader); err != nil {
						panic(err)
						//if err := pipeWriter.CloseWithError(err); err != nil {
						//	panic(err)
						//}
						return
					}

					if _, err := io.Copy(tarWriter, tarReader); err != nil {
						panic(err)
						//if err := pipeWriter.CloseWithError(err); err != nil {
						//	panic(err)
						//}
						return
					}
					continue
				}

				fileBuffer := &bytes.Buffer{}
				hasher := md5.New()
				multiWriter := io.MultiWriter(fileBuffer, hasher)
				if _, err := io.Copy(multiWriter, tarReader); err != nil {
					panic(err)
					//if err := pipeWriter.CloseWithError(err); err != nil {
					//	panic(err)
					//}

					return
				}

				sum := fmt.Sprintf("%x", hasher.Sum(nil))

				existingIdenticalPath := hardLinkSources[sum]
				if existingIdenticalPath != "" {
					fmt.Printf("link: %s => %s (%d)\n", tarHeader.Name, existingIdenticalPath, tarHeader.Size)
					savedSpace += tarHeader.Size

					tarHeader.Typeflag = tar.TypeLink
					tarHeader.Linkname = existingIdenticalPath
					tarHeader.Size = 0

					if err := tarWriter.WriteHeader(tarHeader); err != nil {
						panic(err)
						//if err := pipeWriter.CloseWithError(err); err != nil {
						//	panic(err)
						//}
						return
					}
				} else {
					hardLinkSources[sum] = tarHeader.Name

					if err := tarWriter.WriteHeader(tarHeader); err != nil {
						panic(err)
						//if err := pipeWriter.CloseWithError(err); err != nil {
						//	panic(err)
						//}
						return
					}

					if _, err := io.Copy(tarWriter, fileBuffer); err != nil {
						panic(err)
						//if err := pipeWriter.CloseWithError(err); err != nil {
						//	panic(err)
						//}
						return
					}
				}
			}
		}()

		return ioutil.NopCloser(pipeReader), nil
	}
}

//TODO: compare buffered to unbuffered
