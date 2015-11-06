using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace ProcessTaskData
{
    class Program
    {
        static void Main(string[] args)
        {
            string blobName = args[0];
            Uri blobUri = new Uri(blobName);
            int numTopN = int.Parse(args[1]);

            CloudBlockBlob blob = new CloudBlockBlob(blobUri);
            string content = blob.DownloadText();
            string[] words = content.Split(' ');
            var topNWords =
              words.
                Where(word => word.Length > 0).
                GroupBy(word => word, (key, group) => new KeyValuePair<String, long>(key, group.LongCount())).
                OrderByDescending(x => x.Value).
                Take(numTopN).
                ToList();

            foreach (var pair in topNWords)
            {
                Console.WriteLine("{0} {1}", pair.Key, pair.Value);
            }
        }
    }
}
