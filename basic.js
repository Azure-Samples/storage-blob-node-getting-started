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

var fs = require('fs');
var util = require('util');
var guid = require('node-uuid');
var crypto = require('crypto');
var storage = require('azure-storage');
var config = require('./config');

function basicSamples() {
  return scenarios = [
    {
      action: basicStorageBlockBlobOperations,
      message: 'Block Blob Sample Completed\n'
    },
    {
      action: basicStoragePageBlobOperations,
      message: 'Page Blob Sample Completed\n'
    }
  ];
}

/**
* Page blob basics.
* @ignore
* 
* @param {errorOrResult}        callback                         The callback function.
*/
function basicStorageBlockBlobOperations(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  var imageToUpload = "HelloWorld.png";
  var blockBlobContainerName = "demoblockblobcontainer-" + guid.v1();
  var blockBlobName = "demoblockblob-" + imageToUpload;

  console.log('Block Blob Sample');

  // Create a container for organizing blobs within the storage account.
  console.log('1. Creating Container');
  blobService.createContainerIfNotExists(blockBlobContainerName, function (error) {
    if (error) return callback(error);

    // Upload a BlockBlob to the newly created container
    console.log('2. Uploading BlockBlob');
    blobService.createBlockBlobFromLocalFile(blockBlobContainerName, blockBlobName, imageToUpload, function (error) {
      if (error) return callback(error);

      // List all the blobs in the container
      console.log('3. List Blobs in Container');
      listBlobs(blobService, blockBlobContainerName, null, null, null, function (error, results) {
        if (error) return callback(error);

        for (var i = 0; i < results.length; i++) {
          console.log(util.format('   - %s (type: %s)'), results[i].name, results[i].blobType);
        }

        // Download a blob to your file system
        console.log('4. Download Blob');
        var downloadedImageName = util.format('CopyOf%s', imageToUpload);
        blobService.getBlobToLocalFile(blockBlobContainerName, blockBlobName, downloadedImageName, function (error) {
          if (error) return callback(error);

          // Create a read-only snapshot of the blob
          console.log('5. Create a read-only snapshot of the blob');
          blobService.createBlobSnapshot(blockBlobContainerName, blockBlobName, function (error, snapshotId) {
            if (error) return callback(error);

            // Create three new blocks and upload them to the existing blob
            console.log('6. Create three new blocks and upload them to the existing blob');
            var buffer = getRandomBuffer(1024);
            var blockIds = [];
            var blockCount = 0;
            var blockId = getBlockId(blockCount);
            var uploadBlockCallback = function (error) {
              if (error) return callback(error);

              blockCount++;
              if (blockCount <= 3) {
                blockId = getBlockId(blockCount);
                blockIds.push(blockId);
                blobService.createBlockFromText(blockId, blockBlobContainerName, blockBlobName, buffer, uploadBlockCallback);
              } else {
                // Important: Please make sure that you call commitBlocks in order to commit the blocks to the blob
                var blockList = { 'UncommittedBlocks': blockIds };
                blobService.commitBlocks(blockBlobContainerName, blockBlobName, blockList, function (error) {

                  // Clean up after the demo. Deleting blobs are not necessary if you also delete the container. The code below simply shows how to do that.
                  console.log('7. Delete block Blob and all of its snapshots');
                  var deleteOption = { deleteSnapshots: storage.BlobUtilities.SnapshotDeleteOptions.BLOB_AND_SNAPSHOTS };
                  blobService.deleteBlob(blockBlobContainerName, blockBlobName, deleteOption, function (error) {
                    try { fs.unlinkSync(downloadedImageName); } catch (e) { }
                    if (error) return callback(error);

                    // Delete the container
                    console.log('8. Delete Container');
                    blobService.deleteContainerIfExists(blockBlobContainerName, function (error) {
                      callback(error);
                    });
                  });
                });
              }
            };

            blockIds.push(blockId);
            blobService.createBlockFromText(blockId, blockBlobContainerName, blockBlobName, buffer, uploadBlockCallback);
          });
        });
      });
    });
  });
}

/**
* Page blob basics.
* @ignore
* 
* @param {errorOrResult}        callback                         The callback function.
*/
function basicStoragePageBlobOperations(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  var fileToUpload = "HelloPage.dat";
  var pageBlobContainerName = "demopageblobcontainer-" + guid.v1();
  var pageBlobName = "demopageblob-" + fileToUpload;

  console.log('Page Blob Sample');

  // Create a container for organizing blobs within the storage account.
  console.log('1. Creating Container');
  blobService.createContainerIfNotExists(pageBlobContainerName, function (error) {
    if (error) return callback(error);

    // To view the uploaded blob in a browser, you have two options. The first option is to use a Shared Access Signature (SAS) token to delegate 
    // access to the resource. See the documentation links at the top for more information on SAS. The second approach is to set permissions 
    // to allow public access to blobs in this container. Uncomment the line below to use this approach. Then you can view the image 
    // using: https://[InsertYourStorageAccountNameHere].blob.core.windows.net/demopageblobcontainer-[guid]/demopageblob-HelloPage.dat

    // Upload a PageBlob to the newly created container
    console.log('2. Uploading PageBlob');
    blobService.createPageBlobFromLocalFile(pageBlobContainerName, pageBlobName, fileToUpload, function (error) {
      if (error) return callback(error);

      // List all the blobs in the container
      console.log('3. List Blobs in Container');
      listBlobs(blobService, pageBlobContainerName, null, null, null, function (error, results) {
        if (error) return callback(error);

        for (var i = 0; i < results.length; i++) {
          console.log(util.format('   - %s (type: %s)'), results[i].name, results[i].blobType);
        }

        // Read a range from a page blob
        console.log('4. Read a Range from a Page Blob');
        var downloadedFileName = util.format('CopyOf%s', fileToUpload);
        var downloadOptions = { rangeStart: 128, rangeEnd: 255 };
        blobService.getBlobToLocalFile(pageBlobContainerName, pageBlobName, downloadedFileName, downloadOptions, function (error) {
          if (error) return callback(error);

          fs.stat(downloadedFileName, function (error, stats) {
            console.log('   Downloaded File Size: %s', stats.size);
            try { fs.unlinkSync(downloadedFileName); } catch (e) { }

            // Clean up after the demo 
            console.log('7. Delete Page Blob');
            blobService.deleteBlob(pageBlobContainerName, pageBlobName, function (error) {
              if (error) return callback(error);

              // Delete the container
              console.log('8. Delete Container');
              blobService.deleteContainerIfExists(pageBlobContainerName, function (error) {
                callback(error);
              });
            });
          });
        });
      });
    });
  });
}

/**
* Lists blobs in the container.
* @ignore
*
* @param {BlobService}        blobService                         The blob service client.
* @param {string}             container                           The container name.
* @param {object}             token                               A continuation token returned by a previous listing operation. 
*                                                                 Please use 'null' or 'undefined' if this is the first operation.
* @param {object}             [options]                           The request options.
* @param {int}                [options.maxResults]                Specifies the maximum number of directories to return per call to Azure ServiceClient. 
*                                                                 This does NOT affect list size returned by this function. (maximum: 5000)
* @param {LocationMode}       [options.locationMode]              Specifies the location mode used to decide which location the request should be sent to. 
*                                                                 Please see StorageUtilities.LocationMode for the possible values.
* @param {int}                [options.timeoutIntervalInMs]       The server timeout interval, in milliseconds, to use for the request.
* @param {int}                [options.maximumExecutionTimeInMs]  The maximum execution time, in milliseconds, across all potential retries, to use when making this request.
*                                                                 The maximum execution time interval begins at the time that the client begins building the request. The maximum
*                                                                 execution time is checked intermittently while performing requests, and before executing retries.
* @param {string}             [options.clientRequestId]           A string that represents the client request ID with a 1KB character limit.
* @param {bool}               [options.useNagleAlgorithm]         Determines whether the Nagle algorithm is used; true to use the Nagle algorithm; otherwise, false.
*                                                                 The default value is false.
* @param {errorOrResult}      callback                            `error` will contain information
*                                                                 if an error occurs; otherwise `result` will contain `entries` and `continuationToken`. 
*                                                                 `entries`  gives a list of directories and the `continuationToken` is used for the next listing operation.
*                                                                 `response` will contain information related to this operation.
*/
function listBlobs(blobService, container, token, options, blobs, callback) {
  blobs = blobs || [];

  blobService.listBlobsSegmented(container, token, options, function (error, result) {
    if (error) return callback(error);

    blobs.push.apply(blobs, result.entries);
    var token = result.continuationToken;
    if (token) {
      console.log('   Received a segment of results. There are ' + result.entries.length + ' blobs on this segment.');
      listBlobs(blobService, container, token, options, blobs, callback);
    } else {
      console.log('   Completed listing. There are ' + blobs.length + ' blobs.');
      callback(null, blobs);
    }
  });
}

/**
* Generates a random bytes of buffer.
* @ignore
*
* @param {int}        size                         The size of the buffer in bytes.
* @return {Buffer}
*/
function getRandomBuffer(size) {
  return crypto.randomBytes(size);
}

/**
* Generates a random ID for the blob block.
* @ignore
*
* @param {int}        index                        The index of the block.
* @return {string}
*/
function getBlockId(index) {
  var prefix = zeroPaddingString(Math.random().toString(16), 8);
  return prefix + '-' + zeroPaddingString(index, 6);
}

/**
* Adds paddings to a string.
* @ignore
*
* @param {string}     str                          The input string.
* @param {int}        len                          The length of the string.
* @return {string}
*/
function zeroPaddingString(str, len) {
  var paddingStr = '0000000000' + str;
  if (paddingStr.length < len) {
    return zeroPaddingString(paddingStr, len);
  } else {
    return paddingStr.substr(-1 * len);
  }
}

module.exports = basicSamples();