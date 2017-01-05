package cn.cstor.activemq

import javax.jms._

/**
  * Created on 2016/12/8
  *
  * @author feng.wei
  */
object FaceConsumerBitCode {

    def main(args: Array[String]) {
        val connection: Connection = MQUtils.getConnection()

        // Create a Session
        val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

        // Create the destination (Topic or Queue)  ResultImages  bitMq
        val destination: Destination = session.createQueue("ResultImages")

        // Create a MessageConsumer from the Session to the Topic or Queue
        val consumer: MessageConsumer = session.createConsumer(destination)

        while (true) {
            // Wait for a message
            val message: Message = consumer.receive(1000)

            if (message.isInstanceOf[TextMessage]) {
                val textMessage: TextMessage = message.asInstanceOf[TextMessage]
                val text: String = textMessage.getText
                System.out.println("Received: " + text)
            }
            else {
                System.out.println("Received: " + message)
            }
        }

        consumer.close
        session.close
        connection.close
    }

}
