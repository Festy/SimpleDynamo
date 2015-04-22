package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {

/*
5562:177ccecaec32c54b82d5aaafc18a2dadb753e3b1
5556:208f7f72b198dadd244e61801abe1ec3a4857bc9
5554:33d6357cfaaf0f72991b0ecd8c56da066613c089
5558:abf0fd8db03e5ecb199a9b82929e9db79b909643
5560:c25ddd596aa7c81fa12378fa725f706d54325d12 */

    public Uri mUri;
    private static int SERVER_PORT = 10000;
    private static int INSERT_LOCK_TIMEOUT = 1000;
    private static int QUERY_ONE_TIMEOUT = 2000;
    private static int QUERY_ALL_TIMEOUT = 2000;
    private static int DELETE_ONE = 1000;

//    String predecessor=null;
//    String successor=null;

    final static String TAG_CONNECTION = SimpleDynamoActivity.class.getSimpleName()+" connection";
    final static String TAG_LOG = SimpleDynamoActivity.class.getSimpleName()+" log";
    final static String TAG_ERROR = SimpleDynamoActivity.class.getSimpleName()+" error";

    String myPort=null;
    String myID=null;
    String myHash=null;
    String myReplicaOne=null;
    String myReplicaTwo=null;
    Boolean sleep = true;

    HashMap<String,String> idToHash;
    HashMap<String, String> hashToID; // We may not need this ever..
    ArrayList<String> sortedHashList;
    ArrayList<String> idList;
    ConcurrentHashMap<String, String> result;
    long ts;
    int resultCount = 0;
    HashMap<Integer, Object> ackLockMap;
    HashMap<Integer, Boolean> ackBooleanMap;
    int messageID = 0;
    Object resultLock;


    /** Stage One:
     * Storing <k,v> in three nodes. (Failure is taken into account).
     * The node receiving the request will find the coordinator node for that key from it's table.
     * If the connection is successful, it will send the request and get relieved from the responsibility.
     * If the node is offline, the message will be transferred to the next replica node.
     * Since we are only having one failed node at once, this will work for sure.
     *
     * On the receiving coordinator side, if the message will be "write_own" and hasCordinatorFailed is false then you are the coordinator.
     * Store it your own db and pass it two next nodes.
     * Else if hasCoordinatorFailed then you just store it in your own.
     * Else if you get a "write_replica" message then -
     * Either you are seeing the message sent by the coordinator (hence the hasCoordinatorFailed flag will be false) you just have to store it in your own
     * Or hasCoordinatorFailed is true then you have to send the message to the next node as "WRITE_OWN".
     * */

    @Override
    public boolean onCreate() {


        /***************** Telephony *****************/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        Log.e(TAG_LOG, "***************************");
        Log.e(TAG_LOG, "**** OnCreate started "+myPort+" ****");
        Log.e(TAG_LOG, "***************************");
        myID = portStr;
        try {
            myHash = genHash(myID);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG_ERROR,"NoSuchAlgorithmException");
        }
        Log.i(TAG_LOG,"telephony completed");

        resultLock = new Object();
        result = new ConcurrentHashMap<String, String>();
        ackLockMap = new HashMap<Integer, Object>();
        ackBooleanMap = new HashMap<Integer, Boolean>();

        /***************** Database *****************/
        db = new DBHelper(getContext()).getWritableDatabase();
        if(db!=null) {
            Log.i(TAG_LOG,"Database created");
        }
        else {
            Log.e(TAG_ERROR,"Database is null");
        }


        /******** Creating ID -> Hash table **********/
        try {
            idToHash = new HashMap<String,String>();

            idToHash.put("11108",genHash("5554"));
            idToHash.put("11112",genHash("5556"));
            idToHash.put("11116",genHash("5558"));
            idToHash.put("11120",genHash("5560"));
            idToHash.put("11124",genHash("5562"));

            hashToID = new HashMap<String, String>();
            hashToID.put(genHash("5554"),"11108");
            hashToID.put(genHash("5556"),"11112");
            hashToID.put(genHash("5558"),"11116");
            hashToID.put(genHash("5560"),"11120");
            hashToID.put(genHash("5562"),"11124");

            sortedHashList = new ArrayList<String>();
            sortedHashList.add(genHash("5562"));
            sortedHashList.add(genHash("5556"));
            sortedHashList.add(genHash("5554"));
            sortedHashList.add(genHash("5558"));
            sortedHashList.add(genHash("5560"));
//
//            Log.i(TAG_LOG,genHash("5554"));
//            Log.i(TAG_LOG,genHash("5556"));
//            Log.i(TAG_LOG,genHash("5558"));
//            Log.i(TAG_LOG,genHash("5560"));
//            Log.i(TAG_LOG,genHash("5562"));




            Collections.sort(sortedHashList);

            idList = new ArrayList<String>();
            idList.add("11124");
            idList.add("11112");
            idList.add("11108");
            idList.add("11116");
            idList.add("11120");
            Log.i(TAG_LOG,"Hash tables created.");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG_ERROR,"NoSuchAlgorithmException @Hashtables");
        }


        /***************** Replicas *****************/
        // 5562-5556-5554-5558-5560

        switch(myPort)

        {
            case "11108":
                myReplicaOne="11116";
                myReplicaTwo="11120";
                break;
            case "11112":
                myReplicaOne="11108";
                myReplicaTwo="11116";
                break;
            case "11116":
                myReplicaOne="11120";
                myReplicaTwo="11124";
                break;
            case "11120":
                myReplicaOne="11124";
                myReplicaTwo="11112";
                break;
            case "11124":
                myReplicaOne="11112";
                myReplicaTwo="11108";
                break;
            default:
                Log.e(TAG_ERROR,"Some unknown port no:"+myPort);

        }

        Log.i(TAG_LOG,"Replica ports are set"+myPort+" "+myReplicaOne+" "+myReplicaTwo);


        /***************** Server *****************/
        try
        {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,new ServerSocket(SERVER_PORT));
            Log.i(TAG_LOG,"Server is up and running..");
        }
        catch (IOException e){
            e.printStackTrace();
            Log.e(TAG_ERROR,"Server IO Exception");
        }
//        String[] remotePorts = getNeighbours(myPort);
//        Message m = new Message();
//        m.setCoordinator(myPort);
//        m.setSenderPort(myPort);
//        m.setRemortPort(remotePorts[0]);
//        m.setType(Message.TYPE.GET_ME_ALL);
//
//        Message m2 = new Message();
//        m2.setSenderPort(myPort);
//        m2.setRemortPort(remotePorts[1]);
//        m2.setCoordinator(remotePorts[1]);
//        m2.setType(Message.TYPE.GET_ME_ALL);
//
//        Message m3 = new Message();
//        m3.setSenderPort(myPort);
//        m3.setRemortPort(remotePorts[2]);
//        m3.setCoordinator(remotePorts[2]);
//        m3.setType(Message.TYPE.GET_ME_ALL);
//
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m2);
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m3);
        return false;
    }
    private class ServerTask extends AsyncTask<ServerSocket, Message,  Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            Socket socket;
            BufferedInputStream bufferedInputStream;
            ObjectInputStream objectInputStream;

            while (true){
                try
                {
                    socket = serverSocket.accept();
                    bufferedInputStream = new BufferedInputStream(socket.getInputStream());
                    objectInputStream = new ObjectInputStream(bufferedInputStream);
                    Message m = (Message) objectInputStream.readObject();
                    publishProgress(m);
                    objectInputStream.close();
                    socket.close();

                }
                catch (IOException e){
                    e.printStackTrace();
                }
                catch (ClassNotFoundException e){
                    e.printStackTrace();
                }

            }

        }
        protected void onProgressUpdate(Message... msgs){
            Message m = msgs[0];
            Message.TYPE type = m.getType();
            Boolean failure = m.getCoordinatorFailure();

            if(type.equals(Message.TYPE.WRITE_OWN) && failure.equals(false)){

                // 1. Send ACK first and then store it in your own
//                Thread thread = new Thread(new ACKRunnable(m.getMessagID(),m.getSenderPort()));
//                thread.setPriority(Thread.MAX_PRIORITY);
//                thread.run();
                Message ackMessage = new Message();
                ackMessage.setType(Message.TYPE.ACK);
                ackMessage.setMessagID(m.getMessagID());
                ackMessage.setRemortPort(m.getSenderPort());

                Log.i(TAG_LOG,"I am coordinator.. Storing in my db "+m.getKey()+" "+m.getValue());
                ContentValues cv = new ContentValues();
                cv.put("key",m.getKey());
                cv.put("value",m.getValue());
                cv.put("owner",m.getCoordinator());

                Long rowID = db.insertWithOnConflict(TABLE_NAME, null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                if(rowID==-1){
                    Log.e(TAG_ERROR,"Error in db inserting @onProgressUpdate @coordinator");
                }

                Message replica1 = new Message();
                replica1.setCoordinatorFailure(false);
                replica1.setType(Message.TYPE.WRITE_REPLICA);
                replica1.setRemortPort(myReplicaOne);
                replica1.setKey(m.getKey());
                replica1.setValue(m.getValue());
                replica1.setCoordinator(myPort);

                Message replica2 = new Message();
                replica2.setCoordinatorFailure(false);
                replica2.setType(Message.TYPE.WRITE_REPLICA);
                replica2.setRemortPort(myReplicaTwo);
                replica2.setKey(m.getKey());
                replica2.setValue(m.getValue());
                replica2.setCoordinator(myPort);

                Log.i(TAG_LOG,myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ackMessage);
                Log.i(TAG_LOG, "Sending to replica "+myReplicaOne+" "+m.getKey()+" "+m.getValue());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica1);
                Log.i(TAG_LOG, "Sending to replica "+myReplicaTwo+" "+m.getKey()+" "+m.getValue());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica2);

            }
            else if(type.equals(Message.TYPE.WRITE_OWN) && failure.equals(true)){
                Log.i(TAG_LOG,"Replica 1: Failure at coordinator, storing in mine"+m.getKey()+" "+m.getValue());
                ContentValues cv = new ContentValues();
                cv.put("key",m.getKey());
                cv.put("value",m.getValue());
                cv.put("owner",m.getCoordinator());

                Long rowID = db.insertWithOnConflict(TABLE_NAME, null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                if(rowID==-1){
                    Log.e(TAG_ERROR,"Error in db inserting @onProgressUpdate @coordinator");
                }

                Message replica1 = new Message();
                replica1.setCoordinatorFailure(false);
                replica1.setType(Message.TYPE.WRITE_REPLICA);
                replica1.setRemortPort(myReplicaOne);
                replica1.setKey(m.getKey());
                replica1.setValue(m.getValue());

                Log.i(TAG_LOG, "Sending to replica 2"+replica1.getRemortPort()+" "+m.getKey()+" "+m.getValue());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica1);

            }

            else if(type.equals(Message.TYPE.WRITE_REPLICA) && failure.equals(false)){

                Log.i(TAG_LOG,"Storing key values as replication "+m.getKey()+" "+m.getValue());
                ContentValues cv = new ContentValues();
                cv.put("key",m.getKey());
                cv.put("value", m.getValue());
                cv.put("owner",m.getCoordinator());

                Long rowID = db.insertWithOnConflict(TABLE_NAME, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
                if(rowID==-1){
                    Log.e(TAG_ERROR,"Error in db inserting @onProgressUpdate @replicator");
                }
            }

            else if(type.equals(Message.TYPE.READ_ALL)){
                HashMap<String, String>  result= new HashMap<String, String>();
                Log.i(TAG_LOG,"Received request for * from "+m.getSenderPort());

                Cursor cursor = db.rawQuery("select key,value from mytable",null);
                cursor.moveToFirst();
                if(cursor!=null && cursor.getCount()>0){
                    for(int i=0;i<cursor.getCount();i++){
                        result.put(cursor.getString(0),cursor.getString(1));
                        cursor.moveToNext();
                    }
                }

                Log.i(TAG_LOG,"Sending "+result.size()+" keys from my db");
                Message message = new Message();
                message.setType(Message.TYPE.REPLY_ALL);
                message.setResult(result);
                message.setRemortPort(m.getSenderPort());
                message.setMessagID(m.getMessagID());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

            }
            else if(type.equals(Message.TYPE.READ_ONE)){
                Log.i(TAG_LOG, "READ_ONE request received.");
                Cursor cursor = db.rawQuery("select key,value from mytable where key=?",new String[]{m.getKey()});
                if(cursor!=null && cursor.getCount()>0){
                    Message message = new Message();
                    message.setSenderPort(myPort);
                    message.setRemortPort(m.getSenderPort());
                    message.setType(Message.TYPE.REPLY_ONE);
                    message.setKey(m.getKey());
                    message.setMessagID(m.getMessagID());
                    cursor.moveToFirst();
                    message.setValue(cursor.getString(cursor.getColumnIndex("value")));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                }
            }
            else if(type.equals(Message.TYPE.REPLY_ONE)){
                Log.i(TAG_LOG,"Got reply for a key, releasing the lock. ID: "+m.getMessagID());
                result = new ConcurrentHashMap<String,String>();
                result.put(m.getKey(),m.getValue());
                ackBooleanMap.put(m.getMessagID(),true);
                Object lock = ackLockMap.get(m.getMessagID());
                synchronized (lock){
                    lock.notify();
                    Log.i(TAG_LOG,"Lock notified. ID: "+m.getMessagID());
                }

            }
            else if (type.equals(Message.TYPE.REPLY_ALL)){
                result.putAll(m.getResult());
                if(increaseCounter()){
                    resetCounter();
                    Log.i(TAG_LOG,"Got replies from all nodes.");
                    synchronized (resultLock){
                        resultLock.notify();
                        Log.i(TAG_LOG,"Lock notified");
                    }


                }
            }
            else if (type.equals(Message.TYPE.DELETE_ONE)){
                String key = m.getKey();
                db.delete(TABLE_NAME, "key=?", new String[]{key});
            }
            else if(type.equals(Message.TYPE.DELETE_ALL)){
                String key = m.getKey();
                db.delete(TABLE_NAME, null, null);
            }
            else if(type.equals(Message.TYPE.DELETE_CORD)){
                Message message = new Message();
                message.setMessagID(m.getMessagID());
                message.setRemortPort(m.getSenderPort());
                message.setType(Message.TYPE.ACK);
                Log.i(TAG_LOG, "Sending ack for delete_chord request id:" + message.getMessagID());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                delete(null, m.getKey(), null);
            }
            else if(type.equals(Message.TYPE.ACK)){

                int id = m.getMessagID();
                Log.i(TAG_LOG,"Ack received at server for id:"+id);
                ackBooleanMap.put(id,true);
                synchronized (ackLockMap.get(id)){
                    ackLockMap.get(id).notify();
                    Log.i(TAG_LOG,"Lock notified for id: "+id);
                }
                Log.i(TAG_LOG,"Lock notified");
            }
            else if(type.equals(Message.TYPE.GET_ME_ALL)){
                Cursor cursor = db.rawQuery("select key,value from mytable where owner=?",new String[]{m.getCoordinator()});
                HashMap<String,String> map = new HashMap<>();
                if (cursor!=null && cursor.getCount()>0){
                    for(int i=0;i<cursor.getCount();i++)
                    {
                        map.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                        cursor.moveToNext();
                    }
                    Message message = new Message();
                    message.setRemortPort(m.getSenderPort());
                    message.setType(Message.TYPE.RECEIVE_YOU_ALL);
                    message.setResult(map);
                }

            }
            else if(type.equals(Message.TYPE.RECEIVE_YOU_ALL)){
                ContentValues cv;
                HashMap<String,String> map = m.getResult();
                for(Map.Entry entry:map.entrySet()){
                    cv = new ContentValues();
                    cv.put("key",(String)entry.getKey());
                    cv.put("value",(String)entry.getValue());
                    cv.put("owner",m.getCoordinator());
                    db.insertWithOnConflict(TABLE_NAME,"",cv,SQLiteDatabase.CONFLICT_REPLACE);
                }
            }


        }


    }
    private class ClientTask extends AsyncTask<Message, Void, Void>{
        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            ObjectOutputStream objectOutputStream;
            BufferedOutputStream bufferedOutputStream;
            Socket socket = null;
            {
                try{
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg.getRemortPort()));
                    bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                    objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
                    objectOutputStream.writeObject(msg);
                    objectOutputStream.flush();
                    objectOutputStream.close();
                    socket.close();

                }
                catch (UnknownHostException e){
                    e.printStackTrace();
                }
                catch (IOException e){
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getCoordinatorPort(String key){
        try {
            String hash = genHash(key);
            for(int i=0;i<5;i++){
                if(hash.compareTo(sortedHashList.get(i))<=0){
                    return idList.get(i);
                }
            }
            return idList.get(0);

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG_ERROR, "NoSuchAlgorithmException @getCoordinator");
            e.printStackTrace();
            return null;
        }

    }
    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String key = selection;

        switch (key){
            case "\"@\"":
                db.delete(TABLE_NAME,null,null);
                break;
            case "\"*\"":
                db.delete(TABLE_NAME,null,null);

                for(String remotePort:idList){
                    if(!remotePort.equals(myPort)){
                        Message message = new Message();
                        message.setType(Message.TYPE.DELETE_ALL);
                        message.setSenderPort(myPort);
                        message.setRemortPort(myReplicaOne);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
                    }
                }
                break;
            default:
                String coordinator = getCoordinatorPort(key);
                if(myPort.equals(coordinator)){

                    db.delete(TABLE_NAME,"key=?",new String[]{key});

                    Message message = new Message();
                    message.setType(Message.TYPE.DELETE_ONE);
                    message.setSenderPort(myPort);
                    message.setRemortPort(myReplicaOne);
                    message.setKey(key);

                    Message message1 = new Message();
                    message1.setType(Message.TYPE.DELETE_ONE);
                    message1.setSenderPort(myPort);
                    message1.setRemortPort(myReplicaTwo);
                    message1.setKey(key);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message1);

                }
                else{
                    int id = messageID++;
                    Message message1 = new Message();
                    message1.setType(Message.TYPE.DELETE_CORD);
                    message1.setSenderPort(myPort);
                    message1.setRemortPort(myReplicaTwo);
                    message1.setKey(key);
                    message1.setMessagID(id);
                    Object lock = new Object();
                    ackBooleanMap.put(id,false);
                    ackLockMap.put(id,lock);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message1);
                    synchronized (lock){
                        try {
                            Log.i(TAG_LOG,"Locking for single delete");
                            lock.wait(DELETE_ONE);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    Log.i(TAG_LOG,"Delete lock released");
                    if (ackBooleanMap.get(id)){
                        Log.i(TAG_LOG,"ack received");
                    }
                    else{
                        Message message = new Message();
                        message.setType(Message.TYPE.DELETE_ONE);
                        message.setSenderPort(myPort);
                        message.setRemortPort(getReplicaOne(coordinator));
                        message.setKey(key);

                        Message message2 = new Message();
                        message2.setType(Message.TYPE.DELETE_ONE);
                        message2.setSenderPort(myPort);
                        message2.setRemortPort(getReplicaOne(getReplicaOne(coordinator)));
                        message2.setKey(key);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message2);
                    }

                }
                break;
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        if(sleep && myPort.equals("11108")){
            try {
                Thread.sleep(4000);
                sleep=false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String coordinator = getCoordinatorPort(key);
        Long rowID;
        if(coordinator.equals(myPort)){
            values.put("owner",myPort);
            rowID = db.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
            if(rowID==-1) Log.e(TAG_ERROR,"single key insertion error");

            Log.i(TAG_LOG,"insert() I am cordi.. Passing info to replicas "+key+" "+value);

            Message message = new Message();
            message.setRemortPort(myReplicaOne);
            message.setType(Message.TYPE.WRITE_REPLICA);
            message.setSenderPort(myPort);
            message.setCoordinatorFailure(false);
            message.setKey(key);
            message.setValue(value);
            message.setCoordinator(coordinator);

            Message message2 = new Message();
            message2.setRemortPort(myReplicaTwo);
            message2.setType(Message.TYPE.WRITE_REPLICA);
            message2.setSenderPort(myPort);
            message2.setCoordinatorFailure(false);
            message2.setKey(key);
            message2.setValue(value);
            message2.setCoordinator(coordinator);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message2);

        }
        else{
            Log.i(TAG_LOG,"Passing insertion to coordinator "+coordinator+" "+key+" "+value);
            int tempMessageID = messageID++;
            Message message = new Message();
            message.setRemortPort(coordinator);
            message.setType(Message.TYPE.WRITE_OWN);
            message.setSenderPort(myPort);
            message.setCoordinatorFailure(false);
            message.setKey(key);
            message.setValue(value);
            message.setMessagID(tempMessageID);
            message.setCoordinator(coordinator);
            ackLockMap.put(tempMessageID,new Object());
            ackBooleanMap.put(tempMessageID,false);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            Log.i(TAG_LOG,"Locking for insertion ACK");
            synchronized (ackLockMap.get(tempMessageID)){
                try {
                    Object o = ackLockMap.get(tempMessageID);
                    o.wait(INSERT_LOCK_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            Log.i(TAG_LOG,"Insertion lock released");
            if(ackBooleanMap.get(tempMessageID)){
                Log.i(TAG_LOG,"Reason: ack received");
            }
            else {
                Log.i(TAG_LOG,"Reason: timeout.");
                Log.e(TAG_ERROR, "Node failure @" + message.getRemortPort());
                message.setRemortPort(getReplicaOne(coordinator));
                message.setType(Message.TYPE.WRITE_OWN);
                message.setCoordinatorFailure(true);
                message.setCoordinator(coordinator);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            }

            // wait for ACK reply. If in 500ms we don't get any reply, then we assume that the coordinator has failed.
            // One complex implementation would be :
            //      Then we send the message to the next alive replica node, and tell her to send the message to her sister replica.
            // Another easy one:
            //      Send two messages, each one to one replica.
            // Implementing the complex one.
        }

		return null;
	}



	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        String key=selection;
        switch(key){
            case "\"*\"":
                Log.i(TAG_LOG,"Received * query");
                increaseCounter();
                Cursor cursor = db.rawQuery("select key,value from mytable",null);
                cursor.moveToFirst();
                if(cursor!=null && cursor.getCount()>0){
                    for(int i=0;i<cursor.getCount();i++){
                        result.put(cursor.getString(0),cursor.getString(1));
                        cursor.moveToNext();
                    }
                }
                Log.i(TAG_LOG,"Added "+result.size()+" keys from my db");
                for(String remotePort:idList){
                    if(remotePort.equals(myPort)){
                        // skip
                    }
                    else{
                        Message m = new Message();
                        m.setType(Message.TYPE.READ_ALL);
                        m.setRemortPort(remotePort);
                        m.setSenderPort(myPort);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

                    }
                }
                Log.i(TAG_LOG,"Locking.. ");
                synchronized (resultLock){
                    try {
                        resultLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Log.i(TAG_LOG,"Lock released");
                Log.i(TAG_LOG,"Creating a cursor to be returned");
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"},result.size());
                for (Map.Entry<String, String> entry:result.entrySet()){
                    matrixCursor.addRow(new Object[]{entry.getKey(),entry.getValue()});
                }

                return matrixCursor;
            case "\"@\"":
                return db.rawQuery("select key,value from mytable",null);
            default:
                String coordinator = getCoordinatorPort(key);
                if(coordinator.equals(myPort)){
                    // Retreive from my DB
                    Log.i(TAG_LOG,"Storing in my db");
                    return db.rawQuery("select key,value from mytable where key=?",new String[]{key});
                }
                else{
                    // Create a message and send it to the coordinator
                    int tempID = messageID++;
                    Object lock = new Object();
                    ackLockMap.put(tempID,lock);
                    lock = ackLockMap.get(tempID);
                    ackBooleanMap.put(tempID,false);
                    Message message = new Message();
                    message.setSenderPort(myPort);
                    message.setRemortPort(coordinator);
                    message.setKey(key);
                    message.setType(Message.TYPE.READ_ONE);
                    message.setCoordinatorFailure(false);
                    message.setMessagID(tempID);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        synchronized (lock){
                            try {
                                Log.i(TAG_LOG,"Locking for single query reply. ID: "+tempID);
                                lock.wait(QUERY_ONE_TIMEOUT);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        Log.i(TAG_LOG,"Lock released. ID: "+tempID);
                    if(ackBooleanMap.get(tempID)){
                        Log.i(TAG_LOG,"Reason: query result received");
                    }
                    else{
                        Log.e(TAG_ERROR,"Node failure @"+coordinator);
                        Message message1 = new Message();
                        tempID = messageID++;
                        message1.setSenderPort(myPort);
                        message1.setRemortPort(getReplicaOne(coordinator));
                        message1.setKey(key);
                        message1.setType(Message.TYPE.READ_ONE);
                        message1.setCoordinatorFailure(false);
                        message1.setMessagID(tempID);
                        ackLockMap.put(tempID,new Object());
                        ackBooleanMap.put(tempID,false);
                        lock = ackLockMap.get(tempID);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message1);
                        synchronized (lock){
                            try {
                                Log.i(TAG_LOG,"Locking for single query reply from replica. ID: "+tempID);
                                resultLock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        Log.i(TAG_LOG,"Lock released. ID: "+tempID);
                    }
                    Log.i(TAG_LOG,"Creating a cursor to be returned");
                    MatrixCursor matrixCursor1 = new MatrixCursor(new String[]{"key","value"},result.size());
                    matrixCursor1.addRow(new Object[]{key,result.get(key)});
                    return matrixCursor1;
                }

        }

	}

    String getReplicaOne(String coordinator){
        switch(coordinator)

        {
            case "11108":
                myReplicaOne="11116";
                myReplicaTwo="11120";
                break;
            case "11112":
                myReplicaOne="11108";
                myReplicaTwo="11116";
                break;
            case "11116":
                myReplicaOne="11120";
                myReplicaTwo="11124";
                break;
            case "11120":
                myReplicaOne="11124";
                myReplicaTwo="11112";
                break;
            case "11124":
                myReplicaOne="11112";
                myReplicaTwo="11108";
                break;
            default:
                Log.e(TAG_ERROR,"Some unknown port no:"+myPort);

        }
        return myReplicaOne;
    }

    String[] getNeighbours(String node){
        //  will return three nodes
        //  First node at index 0 will be replicaOne of the param node, so that the param node can request all keys that belongs to himself as coordinator
        //  Second two nodes at index 1 and 2 are the coordinators for whom the requesting node is a replica.

        String[] result = new String[3];
        result[0] = getReplicaOne(node);
        for(int i=0;i<5;i++){
            if(getReplicaOne(getReplicaOne(idList.get(i))).equals(node))
                result[1] = getReplicaOne(idList.get(i));
                result[2] = (idList.get(i));
        }
        return result;
    }
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
    private synchronized boolean increaseCounter(){
        resultCount++;
        if(resultCount==5) return true;
        else if(resultCount==4){
            try {
                Thread.sleep(QUERY_ALL_TIMEOUT);

                if(resultCount==4) {
                    Log.e(TAG_ERROR, "Releasing query_all lock with node failure");
                    return true;
                }

                else return false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return false;

    }
    private synchronized void resetCounter(){
        resultCount=0;
    }

    private SQLiteDatabase db;
    static final String DB_NAME = "mydb";
    static final String TABLE_NAME = "mytable";
    static final int DB_VERSION = 2;
    static final String CREATE_DB_TABLE = "CREATE TABLE " + TABLE_NAME+
            " ( key STRING PRIMARY KEY,"
            + " value STRING, owner STRING)";

    private static class DBHelper extends SQLiteOpenHelper {

        DBHelper(Context context){
            super(context, DB_NAME, null, DB_VERSION);
        }
        public void onCreate(SQLiteDatabase db){
            db.execSQL(CREATE_DB_TABLE);
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.i(TAG_LOG,"Old version "+db.getVersion());
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);

            onCreate(db);
            Log.i(TAG_LOG, "Old table dropped ");
            Log.i(TAG_LOG, "New version " + db.getVersion());
        }
    }
}
