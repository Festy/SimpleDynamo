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


    public Uri mUri;
    private static int SERVER_PORT = 10000;

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

    int resultCount = 0;

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
        Log.e(TAG_LOG, "***************************");
        Log.e(TAG_LOG, "**** OnCreate started *****");
        Log.e(TAG_LOG, "***************************");

        /***************** Telephony *****************/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        myID = portStr;
        try {
            myHash = genHash(myID);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG_ERROR,"NoSuchAlgorithmException");
        }
        Log.i(TAG_LOG,"telephony completed");

        resultLock = new Object();
        result = new ConcurrentHashMap<String, String>();

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

        Log.i(TAG_LOG,"Replica ports are set");


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

                // 1. Store it in your own
                Log.i(TAG_LOG,"I am coordinator.. Storing in my db "+m.getKey()+" "+m.getValue());
                ContentValues cv = new ContentValues();
                cv.put("key",m.getKey());
                cv.put("value",m.getValue());
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

                Message replica2 = new Message();
                replica2.setCoordinatorFailure(false);
                replica2.setType(Message.TYPE.WRITE_REPLICA);
                replica2.setRemortPort(myReplicaTwo);
                replica2.setKey(m.getKey());
                replica2.setValue(m.getValue());

                Log.i(TAG_LOG, "Sending to replica "+replica1.getRemortPort()+" "+m.getKey()+" "+m.getValue());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica1);
                Log.i(TAG_LOG, "Sending to replica "+replica2.getRemortPort()+" "+m.getKey()+" "+m.getValue());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica2);

            }

            else if(type.equals(Message.TYPE.WRITE_REPLICA) && failure.equals(false)){

                Log.i(TAG_LOG,"Storing key values as replication "+m.getKey()+" "+m.getValue());
                ContentValues cv = new ContentValues();
                cv.put("key",m.getKey());
                cv.put("value",m.getValue());
                Long rowID = db.insertWithOnConflict(TABLE_NAME, null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                if(rowID==-1){
                    Log.e(TAG_ERROR,"Error in db inserting @onProgressUpdate @replicator");
                }
            }

            else if(type.equals(Message.TYPE.READ_ALL)){
                HashMap<String, String>  result= new HashMap<String, String>();
                Log.i(TAG_LOG,"Received request for * from "+m.getSenderPort());

                Cursor cursor = db.rawQuery("select * from mytable",null);
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
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

            }
            else if(type.equals(Message.TYPE.READ_ONE)){
                Log.i(TAG_LOG,"READ_ONE request received.");
                Cursor cursor = db.rawQuery("select * from mytable where key=?",new String[]{m.getKey()});
                if(cursor!=null && cursor.getCount()>0){
                    Message message = new Message();
                    message.setSenderPort(myPort);
                    message.setRemortPort(m.getSenderPort());
                    message.setType(Message.TYPE.REPLY_ONE);
                    message.setKey(m.getKey());
                    cursor.moveToFirst();
                    message.setValue(cursor.getString(cursor.getColumnIndex("value")));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                }
            }
            else if(type.equals(Message.TYPE.REPLY_ONE)){
                Log.i(TAG_LOG,"Got reply for a key, releasing the lock.");
                result = new ConcurrentHashMap<String,String>();
                result.put(m.getKey(),m.getValue());
                synchronized (resultLock){
                    resultLock.notify();
                    Log.i(TAG_LOG,"Lock notified");
                }

            }
            else if (type.equals(Message.TYPE.REPLY_ALL)){
                result.putAll(m.getResult());
                if(increaseCounter()){
                    Log.i(TAG_LOG,"Got replies from five nodes.");
                    synchronized (resultLock){
                        resultLock.notify();
                        Log.i(TAG_LOG,"Lock notified");
                    }


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
                break;
            case "\"*\"":
                break;
            default:
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
            rowID = db.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
            if(rowID==-1) Log.e(TAG_ERROR,"single key insertion error");

            Log.i(TAG_LOG,"Passing info to replicas");

            Message message = new Message();
            message.setRemortPort(myReplicaOne);
            message.setType(Message.TYPE.WRITE_REPLICA);
            message.setSenderPort(myPort);
            message.setCoordinatorFailure(false);
            message.setKey(key);
            message.setValue(value);

            Message message2 = new Message();
            message2.setRemortPort(myReplicaTwo);
            message2.setType(Message.TYPE.WRITE_REPLICA);
            message2.setSenderPort(myPort);
            message2.setCoordinatorFailure(false);
            message2.setKey(key);
            message2.setValue(value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message2);

        }
        else{
            Log.i(TAG_LOG,"Passing insertion to coordinator "+coordinator+" "+key+" "+value);
            Message message = new Message();
            message.setRemortPort(coordinator);
            message.setType(Message.TYPE.WRITE_OWN);
            message.setSenderPort(myPort);
            message.setCoordinatorFailure(false);
            message.setKey(key);
            message.setValue(value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
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
                Cursor cursor = db.rawQuery("select * from mytable",null);
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
                return db.rawQuery("select * from mytable",null);
            default:
                String coordinator = getCoordinatorPort(key);
                if(coordinator.equals(myPort)){
                    // Retreive from my DB
                    Log.i(TAG_LOG,"Storing in my db");
                    return db.rawQuery("select * from mytable where key=?",new String[]{key});
                }
                else{
                    // Create a message and send it to the coordinator
                    Message message = new Message();
                    message.setSenderPort(myPort);
                    message.setRemortPort(coordinator);
                    message.setKey(key);
                    message.setType(Message.TYPE.READ_ONE);
                    message.setCoordinatorFailure(false);


                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        synchronized (resultLock){
                            try {
                                Log.i(TAG_LOG,"Locking for single query reply");
                                resultLock.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        Log.i(TAG_LOG,"Lock released");
                    Log.i(TAG_LOG,"Creating a cursor to be returned");
                    MatrixCursor matrixCursor1 = new MatrixCursor(new String[]{"key","value"},result.size());
                    matrixCursor1.addRow(new Object[]{key,result.get(key)});
                    return matrixCursor1;
                }

        }

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
        else return false;
    }
    private synchronized void resetCounter(){
        resultCount=0;
    }

    private SQLiteDatabase db;
    static final String DB_NAME = "mydb";
    static final String TABLE_NAME = "mytable";
    static final int DB_VERSION = 1;
    static final String CREATE_DB_TABLE = "CREATE TABLE " + TABLE_NAME+
            " ( key STRING PRIMARY KEY,"
            + " value STRING )";
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
