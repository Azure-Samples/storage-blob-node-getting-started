//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

/**
* Azure Storage Blob Sample - Demonstrate how to use the Blob Storage service.
* Blob storage stores unstructured data such as text, binary data, documents or media files.
* Blobs can be accessed from anywhere in the world via HTTP or HTTPS.
*
* Documentation References: 
* - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
* - Getting Started with Blobs - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/
* - Blob Service Concepts - http://msdn.microsoft.com/en-us/library/dd179376.aspx 
* - Blob Service REST API - http://msdn.microsoft.com/en-us/library/dd135733.aspx
* - Blob Service Node API - http://azure.github.io/azure-storage-node/BlobService.html
* - Delegating Access with Shared Access Signatures - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/
* - Storage Emulator - https://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
*/

var fs = require('fs');
var util = require('util');
var guid = require('node-uuid');
var crypto = require('crypto');
var storage = require('azure-storage');

runBlobSamples();

function runBlobSamples() {
  /**
   * Instructions: This sample can be run using either the Azure Storage Emulator that installs as part of this SDK - or by  
   * updating the app.config file with your connection string.
   *
   * To run the sample using the Storage Emulator (default option)
   *      Start the Azure Storage Emulator (once only) by pressing the Start button or the Windows key and searching for it
   *      by typing "Azure Storage Emulator". Select it from the list of applications to start it.
   * 
   * To run the sample using the Storage Service
   *      Open the app.config file and comment out the connection string for the emulator ("useDevelopmentStorage":true) and
   *      set the connection string for the storage service.
   */   
  console.log('\nAzure Storage Blob Sample\n');
  
  var current = 0;
  var scenarios = [
    {
      scenario: basicStorageBlockBlobOperations,
      message: 'Block Blob Sample Completed\n'
    }, 
    {
      scenario: basicStoragePageBlobOperations,
      message: 'Page Blob Sample Completed\n'
    }];
  
  var callback = function (error) {
    if (error) {
      throw error;
    } else {
      console.log(scenarios[current].message); 
      
      current++;
      if (current < scenarios.length) {
        scenarios[current].scenario(callback);
      }
    }
  };
   
  scenarios[current].scenario(callback);
}

// Block blob basics
function basicStorageBlockBlobOperations(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(readConfig().connectionString);

  var imageToUpload = "HelloWorld.png";
  var blockBlobContainerName = "demoblockblobcontainer-" + guid.v1();
  var blockBlobName = "demoblockblob-" + imageToUpload;
  
  console.log('Block Blob Sample');
  
  // Create a container for organizing blobs within the storage account.
  console.log('1. Creating Container');
  blobService.createContainerIfNotExists(blockBlobContainerName, function (error) {
    if (error) {
      callback(error);
    } else {
      // To view the uploaded blob in a browser, you have two options. The first option is to use a Shared Access Signature (SAS) token to delegate 
      // access to the resource. See the documentation links at the top for more information on SAS. The second approach is to set permissions 
      // to allow public access to blobs in this container. Uncomment the line below to use this approach. Then you can view the image 
      // using: https://[InsertYourStorageAccountNameHere].blob.core.windows.net/demoblockblobcontainer-[guid]/demoblockblob-HelloWorld.png
      
      // Upload a BlockBlob to the newly created container
      console.log('2. Uploading BlockBlob');
      blobService.createBlockBlobFromLocalFile(blockBlobContainerName, blockBlobName, imageToUpload, function (error) {
        if (error) {
          callback(error);
        } else {
          // List all the blobs in the container
          console.log('3. List Blobs in Container');
          listBlobs(blobService, blockBlobContainerName, null, null, function (error, results) {
            if (error) {
              callback(error);
            } else {
              for (var i = 0; i < results.length; i++) {
                console.log(util.format('   - %s (type: %s)'), results[i].name, results[i].properties.blobtype);
              }
              
              // Download a blob to your file system
              console.log('4. Download Blob');
              var downloadedImageName = util.format('CopyOf%s', imageToUpload);
              blobService.getBlobToLocalFile(blockBlobContainerName, blockBlobName, downloadedImageName, function (error) {
                if (error) {
                  callback(error);
                } else {
                  // Create a read-only snapshot of the blob
                  console.log('5. Create a read-only snapshot of the blob'); 
                  blobService.createBlobSnapshot(blockBlobContainerName, blockBlobName, function (error, snapshotId) {
                    if (error) {
                      callback(error);
                    } else {
                      // Create three new blocks and upload them to the existing blob
                      console.log('6. Create three new blocks and upload them to the existing blob');
                      var buffer = getRandomBuffer(1024);
                      var blockIds = [];
                      var blockCount = 0;
                      var blockId = getBlockId(blockCount);
                      var uploadBlockCallback = function (error) {
                        if (error) {
                          callback(error);
                        } else {
                          blockCount++;
                          if (blockCount <= 3) {
                            blockId = getBlockId(blockCount);
                            blockIds.push(blockId);
                            blobService.createBlockFromText(blockId, blockBlobContainerName, blockBlobName, buffer, uploadBlockCallback);
                          } else {
                            // Important: Please make sure that you call commitBlocks in order to commit the blocks to the blob
                            var blockList = { 'UncommittedBlocks': blockIds };
                            blobService.commitBlocks(blockBlobContainerName, blockBlobName, blockList, function (error) {
                              
                              // Clean up after the demo 
                              console.log('7. Delete block Blob and all of its snapshots');
                              var deleteOption = { deleteSnapshots: storage.BlobUtilities.SnapshotDeleteOptions.BLOB_AND_SNAPSHOTS };
                              blobService.deleteBlob(blockBlobContainerName, blockBlobName, deleteOption, function (error) {
                                try { fs.unlinkSync(downloadedImageName); } catch (e) { }
                                if (error) {
                                  callback(error);
                                } else {
                                  // Delete the container
                                  console.log('8. Delete Container');
                                  blobService.deleteContainerIfExists(blockBlobContainerName, function (error) {
                                    callback(error);
                                  });
                                }
                              });
                            });
                          }
                        }
                      };
                      
                      blockIds.push(blockId);
                      blobService.createBlockFromText(blockId, blockBlobContainerName, blockBlobName, buffer, uploadBlockCallback);
                    }
                  });
                }
              });
            }
          });
        }
      });
    }
  });
}

// Page blob basics
function basicStoragePageBlobOperations(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(readConfig().connectionString);

  var fileToUpload = "HelloPage.dat";
  var pageBlobContainerName = "demopageblobcontainer-" + guid.v1();
  var pageBlobName = "demopageblob-" + fileToUpload;
  
  console.log('Page Blob Sample');
  
  // Create a container for organizing blobs within the storage account.
  console.log('1. Creating Container');
  blobService.createContainerIfNotExists(pageBlobContainerName, function (error) {
    if (error) {
      callback(error);
    } else {
      // To view the uploaded blob in a browser, you have two options. The first option is to use a Shared Access Signature (SAS) token to delegate 
      // access to the resource. See the documentation links at the top for more information on SAS. The second approach is to set permissions 
      // to allow public access to blobs in this container. Uncomment the line below to use this approach. Then you can view the image 
      // using: https://[InsertYourStorageAccountNameHere].blob.core.windows.net/demopageblobcontainer-[guid]/demopageblob-HelloPage.dat
      
      // Upload a PageBlob to the newly created container
      console.log('2. Uploading PageBlob');
      blobService.createPageBlobFromLocalFile(pageBlobContainerName, pageBlobName, fileToUpload, function (error) {
        if (error) {
          callback(error);
        } else {
          // List all the blobs in the container
          console.log('3. List Blobs in Container');
          listBlobs(blobService, pageBlobContainerName, null, null, function (error, results) {
             if (error) {
              callback(error);
            } else {
              for (var i = 0; i < results.length; i++) {
                console.log(util.format('   - %s (type: %s)'), results[i].name, results[i].properties.blobtype);
              }
              
              // Read a range from a page blob
              console.log('4. Read a Range from a Page Blob');
              var downloadedFileName = util.format('CopyOf%s', fileToUpload);
              var downloadOptions = { rangeStart: 128, rangeEnd: 255 };
              blobService.getBlobToLocalFile(pageBlobContainerName, pageBlobName, downloadedFileName, downloadOptions, function (error) {
                if (error) {
                  callback(error);
                } else {
                  fs.stat(downloadedFileName, function(error, stats) {
                    console.log('   Downloaded File Size: %s', stats.size);
                    try { fs.unlinkSync(downloadedFileName); } catch (e) { }
                    // Clean up after the demo 
                    console.log('7. Delete Page Blob');
                    blobService.deleteBlob(pageBlobContainerName, pageBlobName, function (error) {
                      if (error) {
                        callback(error);
                      } else {
                        // Delete the container
                        console.log('8. Delete Container');
                        blobService.deleteContainerIfExists(pageBlobContainerName, function (error) {
                          callback(error);
                        });
                      }
                    });
                  });
                }
              });
            }
          });
        }
      });
    }
  });
}

function listBlobs (blobService, container, options, token, callback) {
  var blobs = [];
  
  blobService.listBlobsSegmented(container, token, options, function(error, result) {
    blobs.push.apply(blobs, result.entries);
    var token = result.continuationToken;
    if (token) {
      console.log('   Received a page of results. There are ' + result.entries.length + ' blobs on this page.');
      listBlobs(blobService, container, options, token, callback);
    } else {
      console.log('   Completed listing. There are ' + blobs.length + ' blobs.');
      callback(null, blobs);
    }
  });
}

function getRandomBuffer(size) {
  return crypto.randomBytes(size);
}

function getBlockId(index) {
  var prefix = zeroPaddingString(Math.random().toString(16), 8);
  return prefix + '-' + zeroPaddingString(index, 6);
}

function zeroPaddingString(str, len) {
  var paddingStr = '0000000000' + str;
  if(paddingStr.length < len) {
    return zeroPaddingString(paddingStr, len);
  } else {
    return paddingStr.substr(-1 * len);
  }
}

function readConfig() {
  var config = JSON.parse(fs.readFileSync('app.config', 'utf8'));
  if (config.useDevelopmentStorage) {
    config.connectionString = storage.generateDevelopmentStorageCredendentials();
  }
  return config;
}
