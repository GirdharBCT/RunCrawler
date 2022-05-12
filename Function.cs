using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AWSSDK;
using Amazon.Lambda.Core;
using Amazon.Glue;
using Amazon.Glue.Model;
 
using System.Text.Json;
using System.Threading;
using Newtonsoft.Json;
using System.IO;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace RunCrawler
{
    public class Function
    {
        IAmazonGlue GlueClient { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            GlueClient = new AmazonGlueClient();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="glueClient"></param>
        public Function(IAmazonGlue glueClient)
        {
            this.GlueClient = glueClient;
        }
        
        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public class HttpResponse
        {
            public int httpStatusCode { get; set; }
            public string body { get; set; }
        }
        
        public async Task<HttpResponse> FunctionHandler(ILambdaContext context)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            int MAX_RETRY = 20;
            var request = new StartCrawlerRequest
            {
                Name = "Demo-DVT-Crawler"
            };
            var response = await GlueClient.StartCrawlerAsync(request);
            var retry_count = 1;
            var crawler_status = new GetCrawlerResponse();
            while (retry_count < MAX_RETRY)
            {
                //Thread.Sleep(30);
                await Task.Delay(5000);
                LambdaLogger.Log("MY Current Counter"+ retry_count.ToString());
                LambdaLogger.Log(crawler_status?.Crawler?.State?.Value);
                crawler_status = await GlueClient.GetCrawlerAsync(new GetCrawlerRequest { Name = "Demo-DVT-Crawler" });
                LambdaLogger.Log(crawler_status?.Crawler?.State?.Value);
                //LambdaLogger.Log(crawler_status.ToString());
                var crawler_run_status = crawler_status.Crawler.State.Value;
                if (crawler_run_status == "READY")
                {
                    break;
                }
                retry_count += 1;
            }
            context.Logger.Log($"Crawler_status.tostring() -> {crawler_status.ToString()}");
            
            // the code that you want to measure comes here
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            context.Logger.Log($"\nTime Elapsed {elapsedMs}");
            return new HttpResponse 
            { 
                httpStatusCode = 200,
                body="Crawler Completes."
            };


            //var request = new GetCrawlerRequest 
            //{ 
            //    Name = "Demo-DVT-Crawler" 
            //};
            //var response = await GlueClient.GetCrawlerAsync(request);
            //LambdaLogger.Log($"response-->\n{response}");
            //LambdaLogger.Log($"response.tostring-->\n{response.ToString()}");
            //var temp = response.Crawler.State;
            //LambdaLogger.Log(temp);
            //LambdaLogger.Log($"response.type-->\n{typeof(response)}");

            //var temp= JsonConvert.DeserializeObject(response);
            //return JsonSerializer.Deserialize<String>(response)!;

        }
    }
}
