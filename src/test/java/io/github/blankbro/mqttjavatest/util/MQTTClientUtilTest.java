package io.github.blankbro.mqttjavatest.util;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;

import java.util.UUID;

public class MQTTClientUtilTest {

    @Test
    public void testSSL() throws Exception {
        MQTTClientUtil mqttClientUtil = new MQTTClientUtil(
                "tcp://127.0.0.1:1883",
                UUID.randomUUID().toString().replaceAll("-", ""),
                "test",
                "test",
                SSLUtil.getSocketFactory(
                        "caPath",
                        "clientCrtPath",
                        "clientKeyPath",
                        "sslPassword"
                )
        );

        MqttMessage mqttMessage = new MqttMessage("test".getBytes());
        mqttMessage.setQos(0);
        mqttClientUtil.publish("/test", mqttMessage, 1);
    }

    @Test
    public void test() throws Exception {
        MQTTClientUtil mqttClientUtil = new MQTTClientUtil(
                "tcp://127.0.0.1:1883",
                UUID.randomUUID().toString().replaceAll("-", ""),
                "test",
                "test",
                null
        );

        MqttMessage mqttMessage = new MqttMessage("test".getBytes());
        mqttMessage.setQos(2);
        mqttClientUtil.publish("/p", mqttMessage, 1);
    }
}
