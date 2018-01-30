using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace rasterUploader
{
    internal class Program
    {
        private const int BytesPerPage = 512;
        private static string DirectoryPath { get; set; }
        private static string FilePath { get; set; }
        private static string ContainerReference { get; set; }
        private static string Key { get; set; }
        private static bool OnlyMetadata { get; set; }

        private static void Main(string[] args)
        {
            if (args.Length == 0 || !ParseArgs(args))
            {
                ShowUsage();
                return;
            }

            var storageAccount = CloudStorageAccount.Parse(Key);
            var blobClient = storageAccount.CreateCloudBlobClient();

            Console.WriteLine("INFO: Connecting to Azure");
            var container = blobClient.GetContainerReference(ContainerReference);
            container.CreateIfNotExistsAsync().Wait();
            UploadRasters(container).Wait();
        }

        private static bool ParseArgs(IReadOnlyList<string> args)
        {
            for (var i = 0; i < args.Count; i++)
            {
                switch (args[i].ToLower())
                {
                    case "-directory":
                    case "-d":
                        DirectoryPath = args[i + 1].Replace("\\", "\\\\");
                        break;
                    case "-file":
                    case "-fi":
                        FilePath = args[i + 1].Replace("\\", "\\\\");
                        break;
                    case "-container":
                    case "-cr":
                        ContainerReference = args[i + 1];
                        break;
                    case "-key":
                    case "-k":
                        Key = args[i + 1];
                        break;
                    case "-metadata":
                    case "-m":
                        if (bool.TryParse(args[i + 1], out var b)) OnlyMetadata = b;
                        break;
                }
            }
            if (DirectoryPath != null && FilePath != null) return false;

            return (DirectoryPath != null || FilePath != null) && ContainerReference != null && Key != null;
        }

        private static void ShowUsage()
        {
            Console.WriteLine();
            Console.WriteLine("Usage: rasterUploader -d Directory || -fi File -cr Container -k Key -m Metadata");
            Console.WriteLine("\t -d\tDirectory containing files in envi format (.bin, .hdr and .bin.aux.xml)");
            Console.WriteLine("\t -fi\tFile in envi format (.bin, .hdr and .bin.aux.xml)");
            Console.WriteLine("\t -cr\tAzure container reference for pageblob storage");
            Console.WriteLine("\t -k\tAzure key (enclosed by double quotes)");
            Console.WriteLine("\t -m\tOnly update metadata [true, false]");
            Console.WriteLine();
        }

        private static async Task UploadRasters(CloudBlobContainer container)
        {
            if (DirectoryPath != null)
            {
                var directory = new DirectoryInfo(DirectoryPath);

                var taskList = directory.GetFiles("*.bin").Select(file => UploadPageBlobAsync(container, file))
                    .ToList();

                await Task.WhenAll(taskList);
            }
            else UploadPageBlobAsync(container, new FileInfo(FilePath)).Wait();
        }

        private static async Task UploadPageBlobAsync(CloudBlobContainer container, FileInfo file)
        {
            var pageBlob = container.GetPageBlobReference(file.Name.Replace(".bin", ""));

            if (file.Length % BytesPerPage != 0)
            {
                Console.WriteLine("INFO: File length not a multiple of 512, padding end to comply: " + file.Name);
                PadFile(file);
            }

            if (!OnlyMetadata)
            {
                Console.WriteLine("INFO: Starting upload of file: " + file.Name);
                var fileStream = file.OpenRead();

                await pageBlob.UploadFromStreamAsync(fileStream);
            }

            SetBlobMetadata(pageBlob, file);
        }

        private static void PadFile(FileInfo file)
        {
            var pages = file.Length / BytesPerPage + 1;

            var neededPadding = pages * BytesPerPage - file.Length;

            var padding = new byte[neededPadding];

            var writer = file.OpenWrite();

            writer.Seek(file.Length, SeekOrigin.Begin);
            writer.WriteAsync(padding, 0, (int) neededPadding).Wait();
            writer.Close();
        }

        private static void SetBlobMetadata(CloudBlob pageBlob, FileSystemInfo file)
        {
            Console.WriteLine("INFO: Reading metadata for file: " + file.Name);

            var headerFile = file.FullName.Replace("bin", "hdr");

            var nullvalue = GetNullValue(file);

            if (nullvalue != string.Empty) pageBlob.Metadata["nullvalue"] = nullvalue;

            foreach (var line in File.ReadLines(headerFile))
            {
                var lineSplit = line.Split("=");
                switch (lineSplit[0].Trim())
                {
                    case "samples":
                        pageBlob.Metadata["rowlength"] = lineSplit[1].Trim();
                        break;
                    case "lines":
                        pageBlob.Metadata["columnlength"] = lineSplit[1].Trim();
                        break;
                    case "header offset":
                        pageBlob.Metadata["headeroffset"] = lineSplit[1].Trim();
                        break;
                    case "map info":
                        var mapinfoSplit = lineSplit[1].Trim().Split(",");
                        pageBlob.Metadata["minx"] = mapinfoSplit[3].Trim();
                        pageBlob.Metadata["maxy"] = mapinfoSplit[4].Trim();
                        pageBlob.Metadata["resolution"] = mapinfoSplit[5].Trim();
                        pageBlob.Metadata["crs"] = mapinfoSplit[7].Replace("{", "").Replace("}", "");
                        break;
                    case "data type":
                        switch (lineSplit[1].Trim())
                        {
                            case "1":
                                pageBlob.Metadata["valuelength"] = "1";
                                break;
                            case "2":
                                pageBlob.Metadata["valuelength"] = "2";
                                break;
                            case "4":
                                pageBlob.Metadata["valuelength"] = "4";
                                break;
                            case "5":
                                pageBlob.Metadata["valuelength"] = "8";
                                break;
                        }
                        break;
                }
            }
            pageBlob.Metadata["maxx"] =
            (ParseHeaderDouble(pageBlob.Metadata["minx"]) + ParseHeaderDouble(pageBlob.Metadata["resolution"]) *
             ParseHeaderDouble(pageBlob.Metadata["rowlength"])).ToString(CultureInfo.InvariantCulture);
            pageBlob.Metadata["miny"] =
            (ParseHeaderDouble(pageBlob.Metadata["maxy"]) - ParseHeaderDouble(pageBlob.Metadata["resolution"]) *
             ParseHeaderDouble(pageBlob.Metadata["columnlength"])).ToString(CultureInfo.InvariantCulture);

            pageBlob.SetMetadataAsync().Wait();
        }

        private static string GetNullValue(FileSystemInfo file)
        {
            var xmlFile = file.FullName + ".aux.xml";
            if (!File.Exists(xmlFile)) return string.Empty;
            var xml = XDocument.Parse(File.ReadAllText(xmlFile));
            return xml.Descendants("NoDataValue").First().Value;
        }

        private static double ParseHeaderDouble(string value)
        {
            double.TryParse(value, out var parseValue);

            if (parseValue.ToString(CultureInfo.InvariantCulture) == "0")
                double.TryParse(value.Replace('.', ','), out parseValue);

            return parseValue;
        }
    }
}