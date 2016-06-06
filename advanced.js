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

var guid = require('node-uuid');
var util = require('util');
var storage = require('azure-storage');
var config = require('./config');

function advancedSamples() {
  return scenarios = [
    {
      action: leaseBlob,
      message: 'Blob Lease Sample Completed\n'
    },
    {
      action: leaseContainer,
      message: 'Container Lease Sample Completed\n'
    },
    {
      action: setCorsRules,
      message: 'Cors Rules Sample Completed\n'
    },
    {
      action: containerOperations,
      message: 'Containers Sample Completed\n'
    },
    {
      action: copyBlob,
      message: 'Copy Blob Sample Completed\n'
    },
    {
      action: containerOperationsWithSas,
      message: 'Containers Sample with SAS Completed\n'
    }
  ];
}

function copyBlob(callback) {
  var blobService = storage.createBlobService(config.connectionString);

  var containerName = "demoblobcontainer-" + guid.v1();

  console.log('1. Creating Container');
  blobService.createContainerIfNotExists(containerName, function (error) {
    if (error) return callback(error);

    console.log('2. Creating Text Blob');
    blobService.createBlockBlobFromText(containerName, 'sourceBlob', 'sample data', function (error) {
      if (error) return callback(error);

      var sourceBlobUrl = blobService.getUrl(containerName, 'sourceBlob');

      console.log('3. Copying Text Blob');
      blobService.startCopyBlob(sourceBlobUrl, containerName, 'targetBlob', function (error, result) {
        if (error) return callback(error);

        console.log('status ' + result.copy.status);

        if (result.copy.status === 'pending') {
          console.log('4. Aborting Text Blob Copy');

          blobService.abortCopyBlob(containerName, 'targetBlob', result.copy.id, function (error) {
            if (error) callback(error);

            console.log('Copy operation aborted');

            //Delete container
            console.log('5. Delete Container');
            blobService.deleteContainerIfExists(containerName, function (error) {
              callback(error);
            })
          })
        } else {
          //Delete container
          console.log('4. Delete Container');
          blobService.deleteContainerIfExists(containerName, function (error) {
            callback(error);
          })
        }
      })
    })
  });
}

function containerOperations(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  var containerName = "demoblobcontainer-" + guid.v1();

  console.log('1. Creating Container');

  blobService.createContainerIfNotExists(containerName, function (error) {
    if (error) return callback(error);

    // List all the containers within the storage account.
    console.log('2. List Container');
    listContainers(blobService, null, null, null, function (error, results) {
      if (error) return callback(error);

      for (var i = 0; i < results.length; i++) {
        console.log(util.format('   - %s'), results[i].name);
      }

      //Delete container
      console.log('3. Delete Container');
      blobService.deleteContainerIfExists(containerName, function (error) {
        callback(error);
      });
    });
  });
}

function setCorsRules(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  blobService.getServiceProperties(function (error, properties) {
    if (error) return callback(error);

    properties.Cors = {
      CorsRule: [{
        AllowedOrigins: ['*'],
        AllowedMethods: ['POST', 'GET'],
        AllowedHeaders: ['*'],
        ExposedHeaders: ['*'],
        MaxAgeInSeconds: 3600
      }]
    };

    blobService.setServiceProperties(properties, function (error) {
      return callback(error);
    });
  });
}

function leaseContainer(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  var containerName = "demoleaseblobcontainer-" + guid.v1();

  console.log('1. Create Container');

  blobService.createContainerIfNotExists(containerName, function (error) {
    if (error) return callback(error);

    console.log('2. Acquire Lease on container');
    blobService.acquireLease(containerName, null, { leaseDuration: 15 }, function (error, leaseResult) {
      if (error) return callback(error);

      console.log('3. Delete Container without lease');
      blobService.deleteContainer(containerName, function (error) {
        if (error) {
          console.log('The container can not be deleted. A lease was not specified. ' + error.message);
        }

        console.log('4. Delete Container with lease ' + leaseResult.id);
        blobService.deleteContainer(containerName, { leaseId: leaseResult.id }, function (error) {
          return callback(error);
        })
      });
    });
  });
}

function leaseBlob(callback) {
  // Create a blob client for interacting with the blob service from connection string
  // How to create a storage connection string - http://msdn.microsoft.com/en-us/library/azure/ee758697.aspx
  var blobService = storage.createBlobService(config.connectionString);

  var containerName = "demoleaseblobcontainer-" + guid.v1();
  var blobName = 'exclusive';

  console.log('1. Create Container');
  blobService.createContainerIfNotExists(containerName, function (error) {
    if (error) return callback(error);

    console.log('2. Create blob');
    blobService.createBlockBlobFromText(containerName, blobName, 'blob created', function (error) {
      if (error) return callback(error);

      console.log('3. Acquire lease on blob');
      blobService.acquireLease(containerName, blobName, { leaseDuration: 15 }, function (error, leaseResult) {
        if (error) return callback(error);

        console.log('4. Delete blob without a lease');
        blobService.deleteBlob(containerName, blobName, function (error) {
          if (error) {
            console.log('The blob can not be deleted. A lease was not specified. ' + error.message);
          }

          console.log('5. Delete blob with lease id ' + leaseResult.id);
          blobService.deleteBlob(containerName, blobName, { leaseId: leaseResult.id }, function (error) {
            if (error) return callback(error);

            console.log('6. Delete container');
            blobService.deleteContainer(containerName, function (error) {
              return callback(error);
            })
          })
        })
      })
    })
  });
}

function containerOperationsWithSas(callback) {
  var containerName = "demosasblobcontainer-" + guid.v1();
  var blobName = 'blobsas';

  var blobService = storage.createBlobService(config.connectionString);

  console.log('1. Creating container')
  blobService.createContainerIfNotExists(containerName, function (error) {
    if (error) return callback(error);

    console.log('2. Getting Shared Access Signature for container');
    var startDate = new Date();
    var expiryDate = new Date(startDate);
    expiryDate.setMinutes(startDate.getMinutes() + 30);

    var sharedAccessPolicy = {
      AccessPolicy: {
        Permissions: storage.BlobUtilities.SharedAccessPermissions.READ +
        storage.BlobUtilities.SharedAccessPermissions.WRITE +
        storage.BlobUtilities.SharedAccessPermissions.DELETE +
        storage.BlobUtilities.SharedAccessPermissions.LIST,
        Start: startDate,
        Expiry: expiryDate
      }
    };

    var sas = blobService.generateSharedAccessSignature(containerName, null, sharedAccessPolicy);

    var sharedBlobService = storage.createBlobServiceWithSas(blobService.host, sas);

    console.log('3. Creating blob with Shared Access Signature');
    sharedBlobService.createBlockBlobFromText(containerName, blobName, 'test data', function (error) {
      if (error) return callback(error);

      console.log('4. Listing blobs in container with Shared Access Signature');
      listBlobs(sharedBlobService, containerName, null, null, null, function (error, blobs) {
        if (error) return callback(error);

        console.log('5. Deleting blob with Shared Access Signature');
        sharedBlobService.deleteBlob(containerName, blobName, function (error) {
          if(error) return callback(error);
          
          console.log('6. Deleting container');
          blobService.deleteContainer(containerName, function(error) {
            return callback(error);
          })
        })
      })
    })
  });
}

/**
* Lists containers in account.
* @ignore
*
* @param {BlobService}        blobService                         The blob service client.
* @param {object}             token                               A continuation token returned by a previous listing operation. 
*                                                                 Please use 'null' or 'undefined' if this is the first operation.
* @param {object}             [options]                           The request options.
* @param {LocationMode}       [options.locationMode]                      Specifies the location mode used to decide which location the request should be sent to. 
*                                                                         Please see StorageUtilities.LocationMode for the possible values.
* @param {int}                [options.maxResults]                        Specifies the maximum number of containers to return per call to Azure storage.
* @param {string}             [options.include]                           Include this parameter to specify that the container's metadata be returned as part of the response body. (allowed values: '', 'metadata')
*                                                                         **Note** that all metadata names returned from the server will be converted to lower case by NodeJS itself as metadata is set via HTTP headers and HTTP header names are case insensitive.
* @param {int}                [options.timeoutIntervalInMs]               The server timeout interval, in milliseconds, to use for the request.
* @param {int}                [options.maximumExecutionTimeInMs]          The maximum execution time, in milliseconds, across all potential retries, to use when making this request.
*                                                                         The maximum execution time interval begins at the time that the client begins building the request. The maximum
*                                                                         execution time is checked intermittently while performing requests, and before executing retries.
* @param {string}             [options.clientRequestId]                   A string that represents the client request ID with a 1KB character limit.
* @param {bool}               [options.useNagleAlgorithm]                 Determines whether the Nagle algorithm is used; true to use the Nagle algorithm; otherwise, false.
*                                                                         The default value is false.
* @param {errorOrResult}      callback                                    `error` will contain information
*                                                                         if an error occurs; otherwise `result` will contain `entries` and `continuationToken`. 
*                                                                         `entries`  gives a list of `[containers]{@link ContainerResult}` and the `continuationToken` is used for the next listing operation.
*                                                                         `response` will contain information related to this operation.
*/
function listContainers(blobService, token, options, containers, callback) {
  containers = containers || [];

  blobService.listContainersSegmented(token, options, function (error, result) {
    containers.push.apply(containers, result.entries);
    var token = result.continuationToken;
    if (token) {
      console.log('   Received a page of results. There are ' + result.entries.length + ' containers on this page.');
      listContainers(blobService, token, options, containers, callback);
    } else {
      console.log('   Completed listing. There are ' + containers.length + ' containers.');
      callback(null, containers);
    }
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
    blobs.push.apply(blobs, result.entries);
    var token = result.continuationToken;
    if (token) {
      console.log('   Received a page of results. There are ' + result.entries.length + ' blobs on this page.');
      listBlobs(blobService, container, token, options, blobs, callback);
    } else {
      console.log('   Completed listing. There are ' + blobs.length + ' blobs.');
      callback(null, blobs);
    }
  });
}

module.exports = advancedSamples();
