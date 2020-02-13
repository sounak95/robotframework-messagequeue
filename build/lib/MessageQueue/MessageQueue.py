try:
    import pymqi
    flag = True
except:
    flag = False
from stompest.config import StompConfig
from stompest.sync import Stomp
import os

class MessageQueue(object):
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def _ibm_queue_configure(self, host, port,queue_manager, channel):
        """ To configure IBM Queue"""

        host = str(host)
        port = str(port)
        channel = str(channel)
        if not queue_manager:
            raise AssertionError("queue_manager argument is required.!! Please check and pass queue_manager value")
        if not channel:
            raise AssertionError("channel argument is required.!! Please check and pass channel value")
        conn_info = "%s(%s)" % (host, port)
        qmgr = None
        try:
            qmgr = pymqi.connect(queue_manager, channel, conn_info)
        except Exception as e:
            raise AssertionError("Exception : {}".format(e))
        return qmgr

    def _active_queue_configure(self,host,port,user_name,password):
        """ TO connect to Active MQ """
        host = str(host)
        port = str(port)
        user_name = str(user_name)
        password = str(password)
        if not user_name:
            raise AssertionError("user_Name argument is required.!! Please check and pass user_Name value")
        if not password:
            raise AssertionError("password argument is required.!! Please check and pass password value")
        ActiveMQ_url = "tcp://{}:{}".format(host, port)
        ActiveMQ_Client = None
        config = StompConfig(ActiveMQ_url, login=user_name, passcode=password)
        ActiveMQ_Client = Stomp(config)
        ActiveMQ_Client.connect()
        return ActiveMQ_Client

    def connect_to_message_queue(self,queue_type,host,port,user_name=None,password=None,queue_manager=None,channel=None):
        """|Usage|  To create connection to the IBM MQ and Active MQ.
            It returns the session which can be used to putting, getting the message and clearing the queue.

                 |Arguments|

                 'queue_type' = It takes "IBM" or "Active" as argument in order to specify the queue type.

                 'host' = It takes the host name for the connection to the queue

                 'port' = It takes the port name for the connection to the queue

                 'user_name' = It takes the user name for the connection. This argument is mandatory for Active MQ but optional for IBM MQ.

                 'password' = It takes the password for the connection. This argument is mandatory for Active MQ but optional for IBM MQ.

                 'queue_manager' = Name of the queue manger used for IBM Connection only. It is mandatory for IBM MQ

                 'channel' = Name of the channel which is mandatory for IBM MQ only

                 Example:
                     ${session}    Connect To Message Queue    Active    ${host}    ${port}    user_Name=${user_name}    password=${password}
                     ${session}    Connect To Message Queue    IBM    ${host}    ${port}   queue_manager=${queue_manager}    channel=${channel}

                """
        if queue_type.upper() == "IBM":
            if not flag:
                raise AssertionError("IBM MQ Client is not installed.Please install IBM MQ Client to work with IBM MQ Messages")
            session = self._ibm_queue_configure(host, port, queue_manager, channel)
        elif queue_type.upper() == "ACTIVE":
            session = self._active_queue_configure(host, port, user_name, password)
        else:
            raise AssertionError("Passed queue type is {} is not supported !!".format(queue_type))
        if not session:
            raise AssertionError("Connection is not established.")
        return session


    def _put_message_in_ibm_queue(self, session, queue_name, message):
        """ It is Used to put the messages in IBM MQ queue. It takes the session instance to perform action."""

        queue_name = str(queue_name)
        queue = pymqi.Queue(session, queue_name)
        queue.put(message)
        queue.close()


    def _put_message_in_active_queue(self, session, queue_name, message, headers=None):
        """ It is Used to put the messages in Active MQ queue.It takes the session instance to perform action."""
        queue_name = str(queue_name)
        if headers != None:
            headers = eval(str(headers))
        session.send(queue_name, message.encode(), headers)


    def put_message_to_queue(self, session, queue_type, queue_name,inputfilepath, headers=None):
        """|Usage|  To put message to the queue.

                 |Arguments|

                 'session' = the return value of the "Connect To Message Queue" keyword.
                 It uses the connection reference to put message to the queue.

                 'queue_type' = It takes "IBM" or "Active" as argument in order to specify the queue type.

                 'queue_name' = Name of IBM MQ or Active MQ queue

                 'inputfilepath' = file path of the message
                  Example:
                      ${session}    Connect To Message Queue    IBM    ${host}    ${port}   queue_manager=${queue_manager}    channel=${channel}
                        Put Message To Queue    ${session}    IBM    ${queue_name}    ${inputfilepath}
        """
        if inputfilepath:
            if not os.path.exists(str(inputfilepath)):
                raise AssertionError('File {} does not exists'.format(inputfilepath))
            with open(inputfilepath, 'r') as f:
                message = f.read()
        try:
            if queue_type.upper() == "IBM":
                self._put_message_in_ibm_queue(session, queue_name, message)
            elif queue_type.upper() == "ACTIVE":
                self._put_message_in_active_queue(session, queue_name, message, headers)
            else:
                raise AssertionError("Passed queue type is {} is not supported !!".format(queue_type))
        except Exception as e:
            raise AssertionError("Exception : {}".format(e))

    def get_message_from_queue(self, queue_type, queue_name, session, uniquemessageid=None, outputfilepath=None):
        """|Usage|  To Get Message From Queue

                 == Arguments ==

                 'queue_type' = It takes "IBM" or "Active" as argument in order to specify the queue type.

                 'queue_name' = Name of queue from which message would be retrieve"

                 'session' = The return value of the "Connect To Message Queue" keyword. It uses the connection reference to get message from the queue.

                 'uniquemessageid' [Optional] = It is an unique message id used to retrieve a particular message from queue. If not provided it returns the first message from the queue.

                 'outputfilepath' [Optional] = It is the filepath to which a retrieve message from the queue could be saved.

                == Example Test Cases ==

                1. To Get The First Message From Active MQ Queue:
                |${session} | Connect To Message Queue | ACTIVE | ${host} | ${port} | user_Name=${user_Name} | password=${password}
                |${message} | Get Message From Queue | ACTIVE | SAMPLE.Q | ${session}

                2. To Get The First Message From IBM MQ Queue:
                |${session} | Connect To Message Queue | IBM | ${host} | ${port} | queue_manager=${queue_manager} | channel=${channel}
                |${message} | Get Message From Queue | IBM | ADVISING | ${session}

                3. To Get The Particular Message From Active MQ Queue Using UniqueuMessageID:
                |${session} | Connect To Message Queue | ACTIVE | ${host} | ${port} | user_Name=${user_Name} | password=${password}
                |${message} | Get Message From Queue | ACTIVE | SAMPLE.Q | ${session} | uniquemessageid="RefID = 00001"

                4. To Get The Particular Message From IBM MQ Queue Using UniqueuMessageID:
                |${session} | Connect To Message Queue | IBM | ${host} | ${port} | queue_manager=${queue_manager} | channel=${channel}
                |${message} | Get Message From Queue | IBM | ADVISING | ${session} | uniquemessageid="RefID = 00001"

                Note: If "uniquemessageid" not provided as an argument to this keyword, it returns the first message from the queue.

                  """
        if outputfilepath:
            if not os.path.exists(str(outputfilepath)):
                raise AssertionError('File {} does not exists'.format(outputfilepath))
        if queue_type.upper() == "IBM":
            message = self._get_message_from_ibm_queue(queue_name, session, uniquemessageid)
        elif queue_type.upper() == "ACTIVE":
            message = self._get_message_from_active_mq_queue(queue_name, session, uniquemessageid)
        else:
            raise AssertionError("Passed queue type is {} is not supported !!".format(queue_type))
        if outputfilepath:
            with open(outputfilepath,'w') as myfile:
                myfile.write(str(message))
                myfile.close()
                print("Message content has been saved to '{}' file".format(outputfilepath))
        return message

    def _get_message_from_ibm_queue(self, queue_name, session, uniquemessageid=None):
        """ It is Used to get the messages from IBM MQ queue. It takes the session instance to perform action."""
        
        if uniquemessageid:
            queue = pymqi.Queue(session, str(queue_name),pymqi.CMQC.MQOO_FAIL_IF_QUIESCING | pymqi.CMQC.MQOO_INPUT_SHARED | pymqi.CMQC.MQOO_BROWSE)
            current_options = pymqi.GMO()
            current_options.Options = pymqi.CMQC.MQGMO_BROWSE_NEXT
            while True:
                try:
                    md = pymqi.MD()
                    message = queue.get(None, md, current_options)
                    find = str(message).find(str(uniquemessageid))
                    if find != -1:
                        break
                except pymqi.MQMIError as e:
                    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        raise AssertionError("No Message With '{0}' Unique Message ID Found in '{1}' Queue".format(uniquemessageid,queue_name))
            queue.close()
        else:
            queue = pymqi.Queue(session, str(queue_name),pymqi.CMQC.MQOO_FAIL_IF_QUIESCING | pymqi.CMQC.MQOO_INPUT_SHARED | pymqi.CMQC.MQOO_BROWSE)
            current_options = pymqi.GMO()
            current_options.Options = pymqi.CMQC.MQGMO_BROWSE_FIRST
            try:
                md = pymqi.MD()
                message = queue.get(None, md, current_options)
            except pymqi.MQMIError as e:
                if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    raise AssertionError("'{}' queue is empty!".format(str(queue_name)))
            finally:
                queue.close()
        print("Message content: {}".format(message))
        return message

    def _get_message_from_active_mq_queue(self, queue_name, session, uniquemessageid=None):
        """ It is Used to get the messages from Active MQ queue.It takes the session instance to perform action."""

        token = session.subscribe(queue_name, {"ack": "client-individual", "id": "0"})
        if not (session.canRead(timeout=2)):
            raise AssertionError("'{}' queue is empty!".format(queue_name))
        if uniquemessageid:
            find = -1
            while(session.canRead(timeout=2)):
                frame = session.receiveFrame()
                find = str(frame.body).find(uniquemessageid)
                if find != -1:
                    break
            if find == -1:
                raise AssertionError("No Message With '{0}' Unique Message ID Found in '{1}' Queue".format(uniquemessageid,queue_name))
        else:
            frame = session.receiveFrame()
        message = frame.body
        session.unsubscribe(token)
        print("Message content : {}".format(message))
        return message

    def _clear_IBM_queue(self, session, queue_name):
        """ It used to clear the IBM MQ queue.This keyword pops all the message in the IBM Queue."""

        queue_name = str(queue_name)
        while True:
            queue = pymqi.Queue(session, queue_name)
            try:
                queue.get()
            except Exception as e:
                if "FAILED: MQRC_UNKNOWN_OBJECT_NAME" in str(e):
                    raise AssertionError("Given queue_name = {} is not present !!!".format(queue_name))
                print("All messages for queue {} have been cleared").format(queue_name)
                break

    def _clear_active_mq(self, session, queue_name):
        """ It used to clear the Active MQ queue.This keyword pops all the message in the Active Queue."""

        queue_name = str(queue_name)
        token = session.subscribe(queue_name, {"ack": "client-individual", "id": "0"})
        while (session.canRead(timeout=2)):
            frame = session.receiveFrame()
            session.ack(frame)
        else:
            print("All messages for queue {} have been cleared").format(queue_name)
        session.unsubscribe(token)

    def clear_queue(self, session, queue_type, queue_name):
        """|Usage|  To clear all messages from IBM or Active MQ

                 |Arguments|

                 'session' = the return value of the "Connect To Message Queue" keyword.
                 It uses the connection reference to put message to the queue.

                 'queue_type' = It takes "IBM" or "Active" as argument in order to specify the queue type.

                 'queue_name' = Name of IBM MQ or Active MQ queue
                  Example:
                      Clear Queue    ${var}    Active    sample.q
                  """
        if queue_type.upper() == "IBM":
            self._clear_IBM_queue(session,queue_name)
        elif queue_type.upper() == "ACTIVE":
            self._clear_active_mq(session,queue_name)
        else:
            raise AssertionError("Passed queue type is {} is not supported !!".format(queue_type))

    def disconnect_message_queue(self, session):
        """|Usage|  To Disconnect Active MQ or IBM MQ
        |Arguments|

                 'session' = the return value of the "Connect To Message Queue" keyword.
                 It uses the connection reference to put message to the queue.

        Example:
            Disconnect From Queue    ${var}
        """
        session.disconnect()









