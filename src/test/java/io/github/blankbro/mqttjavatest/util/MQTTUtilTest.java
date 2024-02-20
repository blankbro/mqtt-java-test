package io.github.blankbro.mqttjavatest.util;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;

import java.util.UUID;

public class MQTTUtilTest {

    @Test
    public void testSSL() throws Exception {
        IMqttAsyncClient mqttClient = MQTTUtil.buildClient("tcp://127.0.0.1:1883"
                , UUID.randomUUID().toString().replaceAll("-", "")
                , "test"
                , "test"
                , SSLUtil.getSocketFactory(
                        "caPath",
                        "clientCrtPath",
                        "clientKeyPath",
                        "sslPassword"
                )
        );

        MqttMessage mqttMessage = new MqttMessage("test".getBytes());
        mqttMessage.setQos(0);
        mqttClient.publish("/test", mqttMessage);
    }

    @Test
    public void test() throws Exception {
        IMqttAsyncClient mqttClient = MQTTUtil.buildClient("tcp://127.0.0.1:1883"
                , UUID.randomUUID().toString().replaceAll("-", "")
                , "test"
                , "test"
                , null
        );

        MqttMessage mqttMessage = new MqttMessage("test".getBytes());
        mqttMessage.setQos(2);
        IMqttDeliveryToken deliveryToken = mqttClient.publish("/p", mqttMessage);
        deliveryToken.getMessage();
        deliveryToken.waitForCompletion(-1);

    }
}
