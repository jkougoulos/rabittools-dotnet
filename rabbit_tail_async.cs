using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using CommandLine;
using CommandLine.Text;

namespace rabbit_tail_async
{

    class Program
    {

        public class Options
        {
            [Option('h', "host", Required = false, HelpText = "Set the RabbitMQ host", Default = "localhost" )]
            public string host { get; set; }

            [Option('u', "user", Required = false, HelpText = "Set the username", Default = "guest" )]
            public string user { get; set; }

            [Option('p', "password", Required = false, HelpText = "Set the password", Default = "guest" )]
            public string password { get; set; }

            [Option('v', "vhost", Required = false, HelpText = "Set the virtual host", Default = "/" )]
            public string vhost{ get; set; }

            [Option('q', "queue", Required = true, HelpText = "Set the queue")]
            public string queue{ get; set; }

            [Option('f', "prefetch", Required = false, HelpText = "Set the prefetch size")]
            public ushort prefetch{ get; set; }

            [Option('a', "autoack", Required = false, HelpText = "Autoacknowledge messages", Default=false)]
            public bool autoack{ get; set; }

        }

        static void Main(string[] args)
        {


		Options o = new Options(); 

	        Parser.Default.ParseArguments<Options>(args)
                  .WithParsed<Options>(opts =>
                   { 
			o = opts;
		   }
		);

		ConnectionFactory factory = new ConnectionFactory() { HostName = o.host, UserName = o.user, Password = o.password, VirtualHost = o.vhost };

		IConnection conn = factory.CreateConnection();
	
		IModel chann = conn.CreateModel();


	
	
	
		var consumer = new EventingBasicConsumer(chann);
	
		consumer.Received += (model, ea  ) =>
		{

			var cons = (EventingBasicConsumer) model;

			var body = ea.Body;
			var bodytxt = Encoding.UTF8.GetString(body);
			bodytxt = bodytxt.TrimEnd('\n');

			Console.WriteLine( bodytxt );	

			if( !o.autoack )
			{
				cons.Model.BasicAck( ea.DeliveryTag, false );
			}
		
		};
	
		if( o.prefetch != 0 )
		{
			chann.BasicQos( 0, o.prefetch , false);
		}
	
		chann.BasicConsume( o.queue, o.autoack, consumer);
	

//		Console.WriteLine(" Press [enter] to exit.");
		Console.ReadLine();

		chann.Close();
		conn.Close();

        }
    }
}
