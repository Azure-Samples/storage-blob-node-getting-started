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

var basicScenarios = require('./basic');
var advancedScenarios = require('./advanced');

runBlobSamples();

function runBlobSamples() {
  /**
   * Instructions: This sample can be run using either the Azure Storage Emulator that installs as part of the Microsoft Azure SDK, which is available in Windows only - or by  
   * updating the app.config file with your connection string.
   *
   * To run the sample using the Storage Emulator (Microsoft Azure SDK)
   *      Start the Azure Storage Emulator (once only) by pressing the Start button or the Windows key and searching for it
   *      by typing "Azure Storage Emulator". Select it from the list of applications to start it.
   * 
   * To run the sample using the Storage Service
   *      Open the app.config file and comment out the connection string for the emulator ("useDevelopmentStorage":true) and
   *      set the connection string for the storage service.
   *
   * Note: The Shared Access Signature sample currently does not work with Emulator for a well know issue in the azure-storage library. If you are planning to run
   * this sample, please use the Storage Service.
   */
  console.log('\nAzure Storage Blob Sample\n');

  var scenarios = basicScenarios.concat(advancedScenarios);

  var current = 0;

  var callback = function (error) {
    if (error) {
      throw error;
    } else {
      console.log(scenarios[current].message);

      current++;
      if (current < scenarios.length) {
        scenarios[current].action(callback);
      }
    }
  };

  scenarios[current].action(callback);
}

