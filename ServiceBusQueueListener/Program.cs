using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace ServiceBusQueueListener
{
    class TokenHandler
    {
        public int count { get; set; } public int reset_epoch { get; set; } 
        public bool canProcess()
        {
            if (count >= 1)
            {
                count--;
                return true;
            }
            else
            {
                return false;
            }
        }

        public int fetch_reset_epoch()
        {
            return reset_epoch;
        }

        public int fetch_count()
        {
            return count;
        }

        public void reset(int epoch)
        {
            reset_epoch = epoch + 60;
            count = 5;
        }


        public TokenHandler()
        {
            reset_epoch = 0; //Can be set to CurrTime.
            count = 5;// LIMIT;
        }
    }

    class Program
    {
        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://vijaysharma-namespace-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=V9kFJKis5RwDKaJESTuy/y45muotm+FxyHfT+PQzRlw=";
        static string queueName = "vijaysharma-sample-queue";
        static ServiceBusClient client;
        // the processor that reads and processes messages from the queue
        static ServiceBusProcessor processor;


        //Event-Hub Settings
        private const string connectionStringEH = "Endpoint=sb://vijaysharma-demo-eventhub.servicebus.windows.net/;SharedAccessKeyName=EventHubRoot;SharedAccessKey=/rxF8nLAbzgADOAg1ZlJTaGzXwmw1pytfairwmshm5g=;EntityPath=vijaysharma-demo-eventhubentity";
        private const string eventHubNameEH = "vijaysharma-demo-eventhubentity";
        private const int numOfEventsEH = 3;
        static EventHubProducerClient producerClient;


        //TokenHandler as RateIdnetifier.
        static TokenHandler tokenMachine;

        static async Task Main()
        {
            tokenMachine = new TokenHandler();
            //SB
            client = new ServiceBusClient(connectionString);
            processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());


            //EH
            producerClient = new EventHubProducerClient(connectionStringEH, eventHubNameEH);

            /* Vijay: to use different sent logic.
            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEventsEH; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }
            */

            try
            {
                // Use the producer client to send the batch of events to the event hub
                //await producerClient.SendAsync(eventBatch);
                //Console.WriteLine($"A batch of {numOfEventsEH} events has been published.");
 

                //Starting of Service Bus Listen.

                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await processor.DisposeAsync();
                await client.DisposeAsync();
                await producerClient.DisposeAsync();
            }
        }

        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            Console.WriteLine($"EPOC- {tokenMachine.fetch_reset_epoch()}   Count- {tokenMachine.fetch_count()}");
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");
            dynamic json_msg = JObject.Parse(body);
            if (json_msg.ContainsKey("status_code"))
            {
                int code = json_msg.status_code;
                int current_epoch = (int)((DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds);
                if(code == 999)
                {
                    if (current_epoch <= tokenMachine.fetch_reset_epoch() && tokenMachine.canProcess())
                    {
                        Console.WriteLine("Pass: " + body);
                    }
                    else
                    {
                        if(current_epoch > tokenMachine.fetch_reset_epoch())
                        {
                            Console.WriteLine("Reseting");
                            tokenMachine.reset(current_epoch);
                        }
                        else
                        {
                            //no token. sent the data to EH
                            json_msg.status_code = 500;
                            IEnumerable<EventData> failed_json = new List<EventData>() { new EventData(Encoding.UTF8.GetBytes(json_msg.ToString())) };
                            await producerClient.SendAsync(failed_json);
                            Console.WriteLine("Alert/Block ===>>>    " + json_msg.ToString());
                        }
                    }
                }
            }
            Thread.Sleep(3000);

            // complete the message. message is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
