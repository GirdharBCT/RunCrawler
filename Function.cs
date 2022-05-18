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
using Amazon.SQS.Model;
using Amazon.SQS;
using Newtonsoft.Json.Linq;
using Amazon.Lambda.SQSEvents;
using static Amazon.Lambda.SQSEvents.SQSEvent;
using Amazon.S3.Util;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace RunCrawler
{
    public class Function
    {
        IAmazonGlue GlueClient { get; set; }
        IAmazonSQS AmazonSQSClient { get; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            GlueClient = new AmazonGlueClient();
            AmazonSQSClient = new AmazonSQSClient();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="glueClient"></param>
        public Function(IAmazonGlue glueClient, IAmazonSQS amazonSQS)
        {
            this.GlueClient = glueClient;
            this.AmazonSQSClient = amazonSQS;
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

        private async Task<ReceiveMessageResponse> GetMessage()
        {
            return await AmazonSQSClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = "https://sqs.ap-south-1.amazonaws.com/436728321574/DVTFileUpload"
            });
        }

        public async Task<HttpResponse> FunctionHandler(ILambdaContext context,Message message)
        {
           


            var recivedMsgResponse = await GetMessage();
            var temp = recivedMsgResponse.Messages[0].Body;
            context.Logger.Log($"sqsEventMessage ->\n{temp}");
            var s3Event = JsonConvert.DeserializeObject<S3EventNotification>(temp);
            var s3Object = s3Event.Records[0].S3.Object;
            context.Logger.Log($"{s3Object}");
            //s3Object = null;
            if (s3Object != null)
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
                    //LambdaLogger.Log("MY Current Counter" + retry_count.ToString());
                    //LambdaLogger.Log(crawler_status?.Crawler?.State?.Value);
                    crawler_status = await GlueClient.GetCrawlerAsync(new GetCrawlerRequest { Name = "Demo-DVT-Crawler" });
                    //LambdaLogger.Log(crawler_status?.Crawler?.State?.Value);
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
                    body = "Crawler Completes."
                };
                //return new HttpResponse
                //{
                //    httpStatusCode = 200,
                //    body = recivedMsgResponse.ToString()
                //};
            }

            return new HttpResponse
            {
                httpStatusCode = 400,
                body = "No new s3 object found"
            };
        }
    }
}
