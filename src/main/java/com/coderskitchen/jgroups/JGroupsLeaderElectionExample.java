package com.coderskitchen.jgroups;

import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class JGroupsLeaderElectionExample extends ReceiverAdapter {

  private static final int MAX_ROUNDS = 1_000;
  private static final int SLEEP_TIME_IN_MILLIS = 1000;
  
  	private int leadperiod;
	private Address currentAddress;
	private Predicate<Address> sameAddress = m -> m.equals(currentAddress);

  public static void main(String[] args) throws Exception {
	  JGroupsLeaderElectionExample member = new JGroupsLeaderElectionExample();
    JChannel channel = new JChannel();
    channel.connect("The Test Cluster");
    channel.setReceiver(member);
    member.currentAddress = channel.getAddress();
    for (int round = 0; round < MAX_ROUNDS; round++) {
      member.checkLeaderStatus(channel);
      sleep();
    }

    channel.close();
  }

  private static void sleep() {
    try {
      Thread.sleep(SLEEP_TIME_IN_MILLIS);
    } catch (InterruptedException e) {
      // Ignored
    }
  }

  private void checkLeaderStatus(JChannel channel) {
	
    View view = channel.getView();
    Address address = view.getMembers()
                          .get(0);
    if (address.equals(currentAddress)) {
      leadperiod++;
      System.out.print("I'm (" + currentAddress + ") the leader for " + leadperiod + " cycles.");
      String members = getMemberList(view, currentAddress);
      System.out.println("other members : " + members);
      if (leadperiod % 4 == 0) {
    	  checkCitizens(channel, view);
      }
    } else {
      System.out.println("I'm (" + currentAddress + ") not the leader");
      leadperiod = 0;
    }
  }
  
  	@Override
	public void receive(Message msg) {
  		System.out.println("Message from :" +msg.getSrc() + " : " + new String(msg.getBuffer()));
	}

	private void checkCitizens(JChannel channel, View view) {
		view.getMembers().stream()
			.filter(sameAddress.negate())
			.map(m -> new Message(m, currentAddress,  "Greets from your master "+channel.getAddressAsString()))
			.forEach(m -> sendMessage(channel, m));
	}
	
	private void sendMessage (JChannel channel, Message message) {
		try {
			channel.send(message);
		} catch (Exception e) {
			System.err.println("citizen not located " + message.getDest().toString());
			e.printStackTrace();
		}
	}
	
	private String getMemberList(View view, Address currentAddress) {
		
		return view.getMembers().stream()
				  .filter(sameAddress.negate())
				  .map(Address::toString)
				  .collect(Collectors.joining(", "));
	}
}
