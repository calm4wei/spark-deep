package cn.cstor.activemq

import javax.jms._

import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Created on 2016/12/8
  *
  * @author feng.wei
  */
object MQUtils {

    def main(args: Array[String]) {
        for (i <- 1 to 10) {
            sendMsg("msg : " + i)
        }
    }

    def getConnection(): Connection = {
        // Create a ConnectionFactory
        val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory("tcp://activemq:61616")

        // Create a Connection
        val connection: Connection = connectionFactory.createConnection
        connection.start

        connection
    }

    def getProducer(): MessageProducer = {

        val connection: Connection = getConnection()
        // Create a Session
        val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

        // Create the destination (Topic or Queue)
        val destination: Destination = session.createQueue("ResultImages")

        // Create a MessageProducer from the Session to the Topic or Queue
        val producer: MessageProducer = session.createProducer(destination)
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)
        producer
    }

    def sendMsg(json: String): Unit = {
        // Create a ConnectionFactory
        val connectionFactory: ActiveMQConnectionFactory = new ActiveMQConnectionFactory("tcp://activemq:61616")

        // Create a Connection
        val connection: Connection = connectionFactory.createConnection
        connection.start

        // Create a Session
        val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

        // Create the destination (Topic or Queue)
        val destination: Destination = session.createQueue("ResultImages")

        // Create a MessageProducer from the Session to the Topic or Queue
        val producer: MessageProducer = session.createProducer(destination)

        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)

        // Create a messages
        val text: String = json
        val message: TextMessage = session.createTextMessage(text)

        // Tell the producer to send the message
        System.out.println("Sent message: " + message.hashCode + " : " + Thread.currentThread.getName)
        producer.send(message)

        // Clean up
        session.close
        connection.close
    }
}
