package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by utsavpatel on 4/21/15.
 */
public class ACKRunnable implements Runnable{
    int messageID;
    String remotePort;

    public ACKRunnable(int messageID, String remotePort){
        this.messageID = messageID;
        this.remotePort = remotePort;
    }
    @Override
    public void run() {
        try {
            Message m = new Message();
            m.setType(Message.TYPE.ACK);
            m.setMessagID(messageID);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            objectOutputStream.writeObject(m);
            objectOutputStream.close();
            socket.close();
            Log.i("SimpleDynamoActivity","Sent ACK reply to"+remotePort);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
