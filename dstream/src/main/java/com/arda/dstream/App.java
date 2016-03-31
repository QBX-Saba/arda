package com.arda.dstream;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Hello world!
 *
 */
public class App implements MqttCallback {
	public static void main(String[] args) {
		try {
			MqttClient client = new MqttClient("tcp://ec2-54-93-53-98.eu-central-1.compute.amazonaws.com:1883", "Listening");
			App app = new App();
			client.connect();
			client.setCallback(app);
			client.subscribe("connectavo.devices.4000002707881.191238");
//			client.sub
			
			// MqttMessage message = new MqttMessage();
			// message.setPayload("A single message from my computer
			// fff".getBytes());
			// client.publish("foo", message);
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub

	}

	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub

	}

	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		System.out.println(arg0+" "+arg1.toString());

	}
}
