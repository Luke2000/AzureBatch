using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;

namespace GettingStarted
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateStorage();
            CreateFiles();

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
                "https://mybatch00.westus.batch.azure.com",
                "mybatch00",
                "YXDbkAXOSLzuPet3NwW0kvjdF2PE6x7Je80qhy1BrDe2VP8PtKxz8q/cyx9Tgox1cdru5g6Lq73qIPotgKcJjA=="
                );
            BatchClient client = BatchClient.Open(cred);

            CreatePool(client);
            ListPools(client);

            CreateJob(client);
            ListJobs(client);

            AddTasks(client);
            ListTasks(client);

            DeleteTasks(client);
            DeleteJob(client);
            DeletePool(client);
        }

        static void CreateStorage()
        {
            // Get the storage connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString"]
                );

            // Create the container
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("testcon1");
            container.CreateIfNotExists();

            // Set permissions on the container
            BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
            containerPermissions.PublicAccess = BlobContainerPublicAccessType.Blob;
            container.SetPermissions(containerPermissions);
            Console.WriteLine("Created the container. Press Enter to continue.");
            Console.ReadLine();
        }

        static void CreateFiles()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
              ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("testcon1");

            CloudBlockBlob taskData1 = container.GetBlockBlobReference("taskdata1");
            CloudBlockBlob taskData2 = container.GetBlockBlobReference("taskdata2");
            CloudBlockBlob taskData3 = container.GetBlockBlobReference("taskdata3");
            taskData1.UploadFromFile("..\\..\\taskdata1.txt", FileMode.Open);
            taskData2.UploadFromFile("..\\..\\taskdata2.txt", FileMode.Open);
            taskData3.UploadFromFile("..\\..\\taskdata3.txt", FileMode.Open);

            CloudBlockBlob storageassembly = container.GetBlockBlobReference("Microsoft.WindowsAzure.Storage.dll");
            storageassembly.UploadFromFile("Microsoft.WindowsAzure.Storage.dll", FileMode.Open);

            CloudBlockBlob dataprocessor = container.GetBlockBlobReference("ProcessTaskData.exe");
            dataprocessor.UploadFromFile(
                "..\\..\\..\\ProcessTaskData\\bin\\debug\\ProcessTaskData.exe",
                FileMode.Open
                );

            Console.WriteLine("Uploaded the files. Press Enter to continue.");
            Console.ReadLine();
        }

        static void CreatePool(BatchClient client)
        {
            CloudPool newPool = client.PoolOperations.CreatePool(
              "testpool1",
              "3",
              "small",
              3
              );
            newPool.Commit();
            Console.WriteLine("Created the pool. Press Enter to continue.");
            Console.ReadLine();
        }

        static void ListPools(BatchClient client)
        {
            IPagedEnumerable<CloudPool> pools = client.PoolOperations.ListPools();
            foreach (CloudPool pool in pools)
            {
                Console.WriteLine("Pool name: " + pool.Id);
                Console.WriteLine("   Pool status: " + pool.State);
            }
            Console.WriteLine("Press enter to continue.");
            Console.ReadLine();
        }

        static CloudJob CreateJob(BatchClient client)
        {
            CloudJob newJob = client.JobOperations.CreateJob();
            newJob.Id = "testjob1";
            newJob.PoolInformation = new PoolInformation() { PoolId = "testpool1" };
            newJob.Commit();
            Console.WriteLine("Created the job. Press Enter to continue.");
            Console.ReadLine();

            return newJob;
        }

        static void ListJobs(BatchClient client)
        {
            IPagedEnumerable<CloudJob> jobs = client.JobOperations.ListJobs();
            foreach (CloudJob job in jobs)
            {
                Console.WriteLine("Job id: " + job.Id);
                Console.WriteLine("   Job status: " + job.State);
            }
            Console.WriteLine("Press Enter to continue.");
            Console.ReadLine();
        }

        static void AddTasks(BatchClient client)
        {
            CloudJob job = client.JobOperations.GetJob("testjob1");
            ResourceFile programFile = new ResourceFile(
                "https://mystorage00.blob.core.windows.net/testcon1/ProcessTaskData.exe",
                "ProcessTaskData.exe"
                );
            ResourceFile assemblyFile = new ResourceFile(
                  "https://mystorage00.blob.core.windows.net/testcon1/Microsoft.WindowsAzure.Storage.dll",
                  "Microsoft.WindowsAzure.Storage.dll"
                  );
            for (int i = 1; i < 4; ++i)
            {
                string blobName = "taskdata" + i;
                string taskName = "mytask" + i;
                ResourceFile taskData = new ResourceFile("https://mystorage00.blob.core.windows.net/testcon1/" +
                  blobName, blobName);
                CloudTask task = new CloudTask(
                    taskName,
                    "ProcessTaskData.exe https://mystorage00.blob.core.windows.net/testcon1/" +
                    blobName + " 3");
                List<ResourceFile> taskFiles = new List<ResourceFile>();
                taskFiles.Add(taskData);
                taskFiles.Add(programFile);
                taskFiles.Add(assemblyFile);
                task.ResourceFiles = taskFiles;
                job.AddTask(task);
                job.Commit();
                job.Refresh();
            }

            client.Utilities.CreateTaskStateMonitor().WaitAll(job.ListTasks(),
        TaskState.Completed, new TimeSpan(0, 30, 0));
            Console.WriteLine("The tasks completed successfully.");
            foreach (CloudTask task in job.ListTasks())
            {
                Console.WriteLine("Task " + task.Id + " says:\n" + task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
            }
            Console.WriteLine("Press Enter to continue.");
            Console.ReadLine();
        }

        static void ListTasks(BatchClient client)
        {
            IPagedEnumerable<CloudTask> tasks = client.JobOperations.ListTasks("testjob1");
            foreach (CloudTask task in tasks)
            {
                Console.WriteLine("Task id: " + task.Id);
                Console.WriteLine("   Task status: " + task.State);
                Console.WriteLine("   Task start: " + task.ExecutionInformation.StartTime);
            }
            Console.ReadLine();
        }

        static void DeleteTasks(BatchClient client)
        {
            CloudJob job = client.JobOperations.GetJob("testjob1");
            foreach (CloudTask task in job.ListTasks())
            {
                task.Delete();
            }
            Console.WriteLine("All tasks deleted.");
            Console.ReadLine();
        }

        static void DeleteJob(BatchClient client)
        {
            client.JobOperations.DeleteJob("testjob1");
            Console.WriteLine("Job was deleted.");
            Console.ReadLine();
        }

        static void DeletePool(BatchClient client)
        {
            client.PoolOperations.DeletePool("testpool1");
            Console.WriteLine("Pool was deleted.");
            Console.ReadLine();
        }
    }
}
