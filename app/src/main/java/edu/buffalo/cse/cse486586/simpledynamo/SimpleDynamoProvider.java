package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

// SimpleDynamoProvider manages the server side and client side data storage activities for all nodes in our distributed system 
public class SimpleDynamoProvider extends ContentProvider {

	public String myPort = ""; //portStr * 2
	public String portStr;
	public String successorID = "";
	public String predecessorID = "";
	public String mySuccessor = "";
	public String myPredecessor = "";
	public String failednode = "";

	boolean blockflag = false;
	ArrayList<String> LiveNodesHash = new ArrayList<String>();
	String[] LiveNodesIDs;
	String thatnodesucc = "";
	BlockingQueue<String> queryblock = new ArrayBlockingQueue<String>(1);
	BlockingQueue<String> queryallblock = new ArrayBlockingQueue<String>(1);
	ArrayList<String> PendingInserts = new ArrayList<String>();
	
	//function to handle delete reguests
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {		
		Context context = getContext();
		File dir =  context.getFilesDir();
		File file= new File(dir,selection);
		file.delete();
		Log.e("Delete", "Deleted key " +selection);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	
	//function to handle insert requests [Check if it belongs to me; else forward to the node it belongs to]
	@Override
	public Uri insert(Uri uri, ContentValues values) {		
		try {
			String keyToInsert = values.get("key").toString();
			String valueToInsert = values.get("value").toString();
			Context context = getContext();
			String key_hash = genHash(keyToInsert);
			Log.e("Inserting key&value", "key: " + keyToInsert + "value: " + valueToInsert);
			
			String portID_hash = genHash(myPort);
			String predecessorID_hash = genHash(predecessorID);

			String  ret = doesItBelongToMe(key_hash, predecessorID_hash, portID_hash, keyToInsert);		//checking if the key belongs to that node 
			Log.v("Insert", keyToInsert + " belongs to " + ret);
			String send_insert_request = "Insert" + "###" + keyToInsert + "###" + valueToInsert + "###" + ret + "###" + successorOfSuccessor(ret);
			
			//Sending it to the Async Task to insert/forward
			new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send_insert_request);
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		}
		return null;
	}

	//checking where the key lies in the chord ring (Consistant hashing slides: https://cse.buffalo.edu/~stevko/courses/cse486/spring16/lectures/14-dht.pdf)
	public String  doesItBelongToMe(String key_hash, String predecessorID_hash, String portID_hash, String keyToInsert) throws NoSuchAlgorithmException {
		String hashNewkey = genHash(keyToInsert);
		if(genHash(LiveNodesIDs[0]).compareTo(hashNewkey) >=0 || genHash(LiveNodesIDs[4]).compareTo(hashNewkey)<0){
			return LiveNodesIDs[0];
		}else if(genHash(LiveNodesIDs[1]).compareTo(hashNewkey)>=0 && genHash(LiveNodesIDs[0]).compareTo(hashNewkey)<0 ){
			return LiveNodesIDs[1];
		}else if(genHash(LiveNodesIDs[2]).compareTo(hashNewkey)>=0 && genHash(LiveNodesIDs[1]).compareTo(hashNewkey)<0 ){
			return LiveNodesIDs[2];
		}else if(genHash(LiveNodesIDs[3]).compareTo(hashNewkey)>=0 && genHash(LiveNodesIDs[2]).compareTo(hashNewkey)<0 ){
			return LiveNodesIDs[3];
		}else if(genHash(LiveNodesIDs[4]).compareTo(hashNewkey)>=0 && genHash(LiveNodesIDs[3]).compareTo(hashNewkey)<0 ){
			return LiveNodesIDs[4];
		}else
			return null;
	}

	//this function is called whenever the nodes come online for the first time or after a failure
	@Override
	public boolean onCreate() {	

		//initializations
		String nodestring = "";
		Log.e("created port:", myPort);

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		//creating a server thread
		int server_port = 10000;
		try {
			ServerSocket serverSocket = new ServerSocket(server_port);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.e("OnCreate", "Server Task initiated");

		} catch (IOException e) {
			Log.e("OnCreate", "Can't create a server socket");
		}
		
		//creating an array to keep a track of other nodes in the system
		try {
			LiveNodesHash.add(genHash("5554"));
			LiveNodesHash.add(genHash("5556"));
			LiveNodesHash.add(genHash("5558"));
			LiveNodesHash.add(genHash("5560"));
			LiveNodesHash.add(genHash("5562"));
			Collections.sort(LiveNodesHash);

			for (String s : LiveNodesHash) {
				nodestring += getIdFromHash(s) + "###";
			}
			LiveNodesIDs = nodestring.split("###");

			System.out.println("LiveNodesHash:  " + LiveNodesHash);
			System.out.println("LiveNodesIDs:  " + LiveNodesIDs[0] + "  " + LiveNodesIDs[1]  + "  " + LiveNodesIDs[2]  + "  " + LiveNodesIDs[3]  + "  " + LiveNodesIDs[4]);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		//sending a request to client thread to send join broadcast to all other nodes
		String join_request = "IAmAlive###" + portStr;       
		Log.e("OnCreate", "sending join request:");
		new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, join_request);

		return false;
	}
	
	//function to handle data retreival requests
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {	
		String readline = null;
		Context context = getContext();
		MatrixCursor matCur = new MatrixCursor(new String[]{"key", "value"});
		String[] list = context.fileList();
		Log.e("QueryProcess", "Query initiated");
		try {
			//if the selection is @ [query all the local data at current node]
			if ((selection.equals("@")))     
			{
				Log.e("QuerySelection", "Case @");
				for (String file : list) {
					InputStream inputStream = context.openFileInput(file);
					if (inputStream != null) {
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader reader = new BufferedReader(inputStreamReader);
						readline = reader.readLine();
						String record[] = {file, readline};
						Log.v("Queryfile", file);
						Log.v("Queryvalue", record[1]);
						matCur.addRow(record);
					}
				}
			}
			//if the selection is * [query all the data stored at all nodes]
			else if((selection.equals("*"))) {	
				//1.return all files from my node
				Log.e("QuerySelection", "Case: forward request for *");
				for (String file : list) {
					InputStream inputStream = context.openFileInput(file);
					if (inputStream != null) {
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader reader = new BufferedReader(inputStreamReader);
						readline = reader.readLine();
						String record[] = {file, readline};
						Log.v("Queryfile", file);
						Log.v("Queryvalue", record[1]);
						matCur.addRow(record);
					}
				}
				//2.query other nodes for their files
				String msg = "QueryAll###" + successorID + "###" + myPort;
				new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
				//using a blocking queue to wait for the results of queries from other nodes
				String allpairs = queryallblock.take(); 
				//splitting the string of keys,values which is of the form: key1<-->value1::key2<-->value2::key3<-->value3 and so on
				String[] pairs = allpairs.split("::");       
				for (String i : pairs) {
					if (!i.equals("") && !(i == null)) {
						String[] keyval = i.split("<-->");
						String key = keyval[0];
						String value = keyval[1];
						String record[] = {key, value};
						matCur.addRow(record);
					}
				}
				return matCur;
			}
			else{		
				//selection is a specific key and the value for that key is requested
				String key= selection;
				String key_hash = genHash(key);

				Log.e("Query key", "key: " + key );
				String portID_hash = genHash(myPort);
				String predecessorID_hash = genHash(predecessorID);

				//checking if the key belongs to that node
				String ret = doesItBelongToMe(key_hash, predecessorID_hash, portID_hash, key);		
				Log.v("Querykey:", "Key " + key + " belongs to " + ret);
				String send_query_request = "Query" + "###" + key + "###" + ret + "###" + successorOfSuccessor(ret);
				new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send_query_request);
				
				//waiting for the results using a blocking queue
				String returnedstring = queryblock.take();
				Log.e("Returned string", returnedstring);
				String[] keyval = returnedstring.split("-->");
				String keyreturned = keyval[0];
				String value = keyval[1];
				String record[] = {keyreturned, value};
				Log.v("Returned Cursor", record[0] + record[1]);
				matCur.addRow(record);
				return matCur;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return matCur;
	}
	
	//checking where the key lies in the chord ring (Consistant hashing slides: https://cse.buffalo.edu/~stevko/courses/cse486/spring16/lectures/14-dht.pdf)
	public int doesItBelongToMeQuery(String key_hash, String predecessorID_hash, String portID_hash, String keyToInsert){
		int ret=-1;
		//if the node and predecessor are on the same side of 0 and if key lies between node and successor
		if ((key_hash.compareTo(predecessorID_hash) > 0) && (key_hash.compareTo(portID_hash) < 0)) 
			ret = 1;
		//if the predecessor and the node lie on different sides of 0 and the key lies after 0 and before the node and the predecessor
		else if ((predecessorID_hash.compareTo(portID_hash) > 0) && ((portID_hash.compareTo(key_hash) > 0) && (predecessorID_hash.compareTo(key_hash)>0)))     
			ret = 1;
		//if the predecessor and the node lie on different sides of 0 and the key lies before 0 and after the node and the predecessor
		else if ((predecessorID_hash.compareTo(portID_hash) > 0) && ((key_hash.compareTo(predecessorID_hash) > 0) && (predecessorID_hash.compareTo(key_hash)<0)))      
			ret = 1;
		//forward the insert to the successor
		else 
			ret = 0;
		return ret;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO UPDATE
		return 0;
	}

	//function to generate Hash Value using the SHA1 hashing algorithm
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
	
	//--------------------------------------------Server thread-------------------------------------------------
	
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		
		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		Uri Uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamno.provider");

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			DataOutputStream outmessage = null;
			DataInputStream inmessage = null;
			String sendqueryout = "";
			String sendqueryout2 = "";
			String succ = "";
			String pred = "";

			Log.e("server", "check");
			
			//Keep  listening...
			while (true) {
				try {
					Socket socket = serverSocket.accept();
					inmessage = new DataInputStream(socket.getInputStream());
					String inmsg = inmessage.readUTF();
					String[] inputstuff = inmsg.split("###");

					//________________________________________________________________---Failed Node Is Back:Server---____________________________________________________________________________________
					if (inputstuff[0].equals("IAmAlive")) {
						String keyval = "KeyVal";
						Log.v("FailureHandling", "Failed node: "+ failednode + "  Pending inserts: " + PendingInserts);

						if (!failednode.equals("") && !PendingInserts.isEmpty()) {
							blockflag = true;
							for (String insert_val :PendingInserts) {
								keyval = keyval + "###" + insert_val;
							}
							Log.v("FailureHandling", "Completing pending inserts: " + keyval);
							PendingInserts.clear();
							publishProgress(keyval);
						}
					}
					//______________________________________________________________________---Failed node Insert Server---____________________________________________________________________________________
					else if (inputstuff[0].equals("PendingInserts")) {
						for(int i=2; i< inputstuff.length; i++) {
							Log.v("Pending inserts", "inserting..." + inputstuff[i]);
							String[] insertkeyval = inputstuff[i].split("-->");
							String key = insertkeyval[0];
							String value = insertkeyval[1];
							FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(value.getBytes());
							outputStream.close();
						}

						outmessage = new DataOutputStream(socket.getOutputStream());
						outmessage.writeUTF("ack");
						outmessage.flush();
					}

					//______________________________________________________________________---Insert Server---____________________________________________________________________________________
					else if (inputstuff[0].equals("Insert")) {
						Log.v("insert server", "inserting key " + inputstuff[1] + " with value " +inputstuff[2]);
						FileOutputStream outputStream = getContext().openFileOutput(inputstuff[1], Context.MODE_PRIVATE);
						outputStream.write(inputstuff[2].getBytes());
						outputStream.close();

						outmessage = new DataOutputStream(socket.getOutputStream());
						outmessage.writeUTF("ack");
						outmessage.flush();
						//______________________________________________________________________---Query Server---____________________________________________________________________________________
					}else if (inputstuff[0].equals("Query")) {
						InputStream input = getContext().openFileInput(inputstuff[1]);
						if (input != null) {
							BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//							String record[] = {request[1], reader.readLine()};
							String query_result = inputstuff[1] + "-->" + reader.readLine();

							outmessage = new DataOutputStream(socket.getOutputStream());
							outmessage.writeUTF(query_result);

						}

					}else if (inputstuff[0].equals("QueryAll")) {       //successor will return all key, value pairs in it's memory and forward the query request
						Cursor cursor2 = query(Uri, null, "@", null, null, null);
						Log.e("ALLQueryServer","cursor created");

						//https://stackoverflow.com/questions/30781603/how-to-retrieve-a-single-row-data-from-cursor-android
						if (cursor2.moveToFirst()) {
							do {

								String key = cursor2.getString(cursor2.getColumnIndex("key"));
								String value = cursor2.getString(cursor2.getColumnIndex("value"));
								sendqueryout2 += key + "<-->" + value + "::";                           //converting the cursor to a string and seperating the key,value pairs using delimitors
							} while (cursor2.moveToNext());
						}
						Log.e("ALLQueryServer",sendqueryout2);


						String suc = successorOfSuccessor(portStr);
						String sendqueries = "StarQueryResults###" + sendqueryout2 + "###" + suc;
						Log.e("ALLQueryOut",sendqueries);
						outmessage = new DataOutputStream(socket.getOutputStream());
						outmessage.writeUTF(sendqueries);
						outmessage.flush();

					}
				} catch (IOException e) {
					Log.e("Server", "IOException");
				}
			}
		}
		protected void onProgressUpdate(String... str) {
			new RequestForward().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, str);
		}
	}

	//-----------------------------------------------Client Thread--------------------------------------------------
	public class RequestForward extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... string) {
			String req = string[0];
			String[] request = req.split("###");
			Socket socket = null;

			if (request[0].equals("IAmAlive")) {
				try {

					for (int i = 5554; i <= 5562; i = i + 2) {
						if (!((String.valueOf(i)).equals(portStr))) {
							Log.v("Recovery", "I am alive to " +i);
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i * 2);
							DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
							for_successor.writeUTF(req);
							for_successor.flush();
							socket.close();

//							DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
//							String inmsg = inmessage.readUTF();
//							inmessage.close();
						}
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			//________________________________________________________________---Failed Node Is Back: Client---____________________________________________________________________________________
			if (request[0].equals("KeyVal")) {
				try {
					Log.e("Failure Handeling", "Sending pending inserts to the recovered node " + failednode);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(failednode) * 2);    //Forwarding insert request
					DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
					String send = "PendingInserts###" + req;
					for_successor.writeUTF(send);
					for_successor.flush();

					DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
					String inmsg = inmessage.readUTF();
					inmessage.close();
					socket.close();
					Log.v("Failure Handeling", "All pending inserts completed " + send);
					failednode = "";
					PendingInserts.clear();
					blockflag = false;

				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}




			}


			//______________________________________________________________________---Insert Client---____________________________________________________________________________________
			else if (request[0].equals("Insert")) {
				String thatnode = request[3];
				thatnodesucc = request[4];

				//inserting to the correct node
				try {
					Log.v("Client", "completing insert request of " + request[1] + " to the correct node: " + thatnode);
					if (portStr.equals(thatnode)) {
						Log.v("insert client", "inserting to the correct node  " + request[1]);
						FileOutputStream outputStream = getContext().openFileOutput(request[1], Context.MODE_PRIVATE);
						outputStream.write(request[2].getBytes());
						outputStream.close();
					} else {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(thatnode) * 2);    //Forwarding insert request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();

						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
						String inmsg = inmessage.readUTF();
						inmessage.close();
						socket.close();

					}

				} catch (UnknownHostException e) {
					Log.e("insertclient", "Host " + mySuccessor + " Failed");
					//TODO Update LiveNodesHash and LiveNodesIDs arrays
				} catch (IOException e) {
					failednode = thatnode;
					Log.e("insertclient", "IOException: Node Failure detected 1");
					String pendinginsert = request[1] + "-->" + request[2];
					PendingInserts.add(pendinginsert);
					System.out.println("Pending Inserts:  " + PendingInserts);
					System.out.println("Failed Node:  " + failednode);

				}

				//Inserting to the successor
				try {
					Log.v("Client", "completing insert request of " + request[1] + " to successor: " + (successorOfSuccessor(thatnode)));
					if (portStr.equals((successorOfSuccessor(thatnode)))) {
						Log.v("insert client", "inserting to the successor node " + request[1]);
						FileOutputStream outputStream = getContext().openFileOutput(request[1], Context.MODE_PRIVATE);
						outputStream.write(request[2].getBytes());
						outputStream.close();
					} else {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt((successorOfSuccessor(thatnode))) * 2);    //Forwarding insert request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();

						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
						String inmsg = inmessage.readUTF();
						inmessage.close();
						socket.close();
					}
				} catch (UnknownHostException e) {
					Log.e("insertclient", "Host " + mySuccessor + " Failed");
					//TODO Update LiveNodesHash and LiveNodesIDs arrays
				} catch (IOException e) {
					failednode = thatnodesucc;
					Log.e("insertclient", "IOException: Node Failure detected 2");
					String pendinginsert = request[1] + "-->" + request[2];
					PendingInserts.add(pendinginsert);
					System.out.println("Pending Inserts:  " + PendingInserts);
					System.out.println("Failed Node:  " + failednode);
				}

				//Inserting to successor's successor
				try {
					Log.v("Client", "completing insert request of " + request[1] + " to successor's successor: " + successorOfSuccessor(successorOfSuccessor(thatnode)));

					if (portStr.equals(successorOfSuccessor(successorOfSuccessor(thatnode)))) {
						Log.v("insert client", "inserting to the successor of successor " + request[1]);
						FileOutputStream outputStream = getContext().openFileOutput(request[1], Context.MODE_PRIVATE);
						outputStream.write(request[2].getBytes());
						outputStream.close();
					} else {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(successorOfSuccessor(thatnode))) * 2);    //Forwarding insert request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();
						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
						String inmsg = inmessage.readUTF();

						inmessage.close();
						socket.close();
					}
				} catch (UnknownHostException e) {
					Log.e("insertclient", "Host " + mySuccessor + " Failed");
					//TODO Update LiveNodesHash and LiveNodesIDs arrays
				} catch (IOException e) {
					failednode = successorOfSuccessor(thatnodesucc);
					Log.e("insertclient", "IOException: Node Failure detected 3");
					String pendinginsert = request[1] + "-->" + request[2];
					PendingInserts.add(pendinginsert);
					System.out.println("Pending Inserts:  " + PendingInserts);
					System.out.println("Failed Node:  " + failednode);
				}
//				insertblock.add("Insert complete");
				blockflag = false;
			}

			//______________________________________________________________________---Query Client---____________________________________________________________________________________

			if (request[0].equals("Query")) {
				String thatnode = request[2];
				thatnodesucc = request[3];
				try {
					Thread.sleep(150);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//inserting to the correct node
				try {
//					while (true) {
//						if(!blockflag)
//							break;
//					}
//					Thread.sleep(30);
					Log.v("Client", "completing query request of " + request[1] + " to  " + successorOfSuccessor(successorOfSuccessor(thatnode)));
					if (portStr.equals(successorOfSuccessor(successorOfSuccessor(thatnode)))) {
						InputStream input = getContext().openFileInput(request[1]);
						if (input != null) {
							BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//							String record[] = {request[1], reader.readLine()};
							String query_result = request[1] + "-->" + reader.readLine();
//							matCur.addRow(record);
							Log.e("querymine", query_result);
							queryblock.put(query_result);
						}
					} else {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(successorOfSuccessor(thatnode))) * 2);    //Forwarding query request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();
						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //string with key and value
						String inmsg = inmessage.readUTF();
						socket.close();
						queryblock.put(inmsg);
						inmessage.close();
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					Log.e("queryclient", "Error:Node Failure detected 1");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (SocketException e) {
					try {
						Log.e("queryclient", "Error: Node Failure detected Socket ");
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(thatnode)) * 2);    //Forwarding query request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();

						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //string with key and value
						String inmsg = inmessage.readUTF();
						socket.close();
						queryblock.put(inmsg);
						inmessage.close();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					} catch (UnknownHostException e1) {
						e1.printStackTrace();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				} catch (IOException e) {
					Log.e("queryclient", "Error:Node Failure detected IO");
					try {
						Log.v("Query Client", "Querying to the successor instead " + successorOfSuccessor(thatnode));
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(thatnode)) * 2);    //Forwarding query request
						DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
						for_successor.writeUTF(req);
						for_successor.flush();

						DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //string with key and value
						String inmsg = inmessage.readUTF();
						socket.close();
						queryblock.put(inmsg);
						inmessage.close();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					} catch (UnknownHostException e1) {
						e1.printStackTrace();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			} else if (request[0].equals("QueryAll")) {
				try {
					String successor = successorOfSuccessor(portStr);
					String originalSender = portStr;
					Log.e(successor, originalSender);
					String pairs = "";
					Log.e("Query", "*");
					while (!successor.equals(originalSender)) {
						try {
							Log.e("QueryOriginPort", originalSender);
							Log.e("Querying next: ", successor);
							Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(successor) * 2));    //forwarding query request
							DataOutputStream for_successor = new DataOutputStream(socket2.getOutputStream());
							for_successor.writeUTF(req);

							DataInputStream inmessage = new DataInputStream(socket2.getInputStream());      //query results received at the node at which the query was initiated
							String inmsg = inmessage.readUTF();
							Log.e("QueryAll", inmsg);
							String[] inputstuff = inmsg.split("###"); //"StarQueryResults",PAIRS, successor

							ArrayList<String> NodesList = new ArrayList<String>(Arrays.asList(LiveNodesIDs));
							int temp = NodesList.indexOf(successor);
							successor = successorOfSuccessor(successor);
							pairs += inputstuff[1];
							inmessage.close();
							socket2.close();
						}catch (SocketException e) {
							Log.e("Skipping failed port: ", successor);
							successor = successorOfSuccessor(successor);
						}catch (IOException e) {
							Log.e("Skipping failed port: ", successor);
							successor = successorOfSuccessor(successor);
						}
					}
					Log.e("QueryAllPairs", pairs);
					queryallblock.put(pairs);       //adding the string of all returned key,value pairs to the blocking queue
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if(request[0].equals("Delete")){
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(myPort)) * 2);    //Forwarding insert request
					DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
					for_successor.writeUTF(req);
					for_successor.flush();

					DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
					String inmsg = inmessage.readUTF();
					inmessage.close();
					socket.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					Log.e("Error", "Node failure delete");
					failednode = successorOfSuccessor(myPort);
					String pendinginsert = request[1] + "-->" + request[2];
					PendingInserts.add(pendinginsert);
					System.out.println("Pending Inserts:  " + PendingInserts);
					System.out.println("Failed Node:  " + failednode);
				}

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorOfSuccessor(successorOfSuccessor(myPort))) * 2);    //Forwarding insert request
					DataOutputStream for_successor = new DataOutputStream(socket.getOutputStream());
					for_successor.writeUTF(req);
					for_successor.flush();

					DataInputStream inmessage = new DataInputStream(socket.getInputStream());      //ack
					String inmsg = inmessage.readUTF();
					inmessage.close();
					socket.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}


			}
			return null;
		}
	}


	public String successorOfSuccessor(String succ) {
		String succ2 = "";

		for (int i = 0; i < LiveNodesIDs.length; i++) {
			if (succ.equals(LiveNodesIDs[i])) {
				if (i == (LiveNodesIDs.length - 1))  //last  node
					succ2 = LiveNodesIDs[0];
				else if (i == 0)                    //first node
					succ2 = LiveNodesIDs[i + 1];
				else
					succ2 = LiveNodesIDs[i + 1];
			}
		}

		return succ2;
	}

	public String getIdFromHash(String hash) {
		String id = "";
		for (int i = 5554; i <= 5562; i += 2) {
			try {
				if ((genHash(String.valueOf(i)).trim()).equals((hash).trim())) {
					id = String.valueOf(i);
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return id;
	}
}

