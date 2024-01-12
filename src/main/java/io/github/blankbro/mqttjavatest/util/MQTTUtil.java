package io.github.blankbro.mqttjavatest.util;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.SocketFactory;

@Slf4j
public class MQTTUtil {

    public static IMqttAsyncClient buildClient(String brokerUrl, String clientId, String username, String password, SocketFactory socketFactory) throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        IMqttAsyncClient mqttClient = new MqttAsyncClient(brokerUrl, clientId, persistence);
        mqttClient.setCallback(new MqttCallbackExtended() {

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("连接成功：reconnect:{}, serverURI:{}", reconnect, serverURI);
            }

            @Override
            public void connectionLost(Throwable cause) {
                log.error("mqtt 连接丢失:{}", cause.getMessage(), cause);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }

            @Override
            public void messageArrived(String topic, MqttMessage arg1) throws Exception {

            }

        });

        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        if (socketFactory != null) {
            // ssl
            mqttConnectOptions.setSocketFactory(socketFactory);
            mqttConnectOptions.setHttpsHostnameVerificationEnabled(false);
        }
        mqttConnectOptions.setCleanSession(true);
        mqttConnectOptions.setUserName(username);
        mqttConnectOptions.setPassword(password.toCharArray());
        // 是否能够断线重连
        mqttConnectOptions.setAutomaticReconnect(true);
        // 检测心跳周期
        mqttConnectOptions.setKeepAliveInterval(60 * 2);
        // 最大并发消息数
        mqttConnectOptions.setMaxInflight(600000);

        log.info("try connect mqtt broker: {} [{}/{}] {} ", clientId, username, password, brokerUrl);
        mqttClient.connect(mqttConnectOptions).waitForCompletion(-1);

        log.info("mqtt 连接成功sync");
        return mqttClient;
    }
}
