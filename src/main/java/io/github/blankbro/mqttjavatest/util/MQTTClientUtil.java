package io.github.blankbro.mqttjavatest.util;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.SocketFactory;

@Slf4j
public class MQTTClientUtil {

    private final IMqttAsyncClient mqttAsyncClient;

    public MQTTClientUtil(String brokerUrl, String clientId, String username, String password, SocketFactory socketFactory) throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        IMqttAsyncClient mqttAsyncClient = new MqttAsyncClient(brokerUrl, clientId, persistence);
        mqttAsyncClient.setCallback(new MqttCallbackExtended() {

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
        mqttAsyncClient.connect(mqttConnectOptions).waitForCompletion(-1);

        log.info("mqtt 连接成功sync");
        this.mqttAsyncClient = mqttAsyncClient;
    }

    public void publish(String topic, String msg, int qos, int retryCount) throws MqttException {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setQos(qos);
        mqttMessage.setPayload(msg.getBytes());
        publish(topic, mqttMessage, retryCount);
    }

    public void publish(String topic, MqttMessage mqttMessage, int retryCount) throws MqttException {
        IMqttDeliveryToken deliveryToken = null;
        for (; retryCount > 0; retryCount--) {
            try {
                // 开始发布消息
                deliveryToken = mqttAsyncClient.publish(topic, mqttMessage);
                // 等待整个链路的完成, 等待时间为 3000ms
                deliveryToken.waitForCompletion(3000);
                // 走到这说明，正常结果
                break;
            } catch (MqttException e) {
                // 如果不是 waitForCompletion timeout 直接抛出
                if (e.getReasonCode() != MqttException.REASON_CODE_CLIENT_TIMEOUT) {
                    throw e;
                }
                // 如果是最后一次重试
                if (retryCount == 1) {
                    // 移除消息, 释放messageId
                    mqttAsyncClient.removeMessage(deliveryToken);
                    throw new RuntimeException("重试3次仍未发送成功");
                }
            }
        }

    }
}
