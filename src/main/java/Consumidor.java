import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumidor {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Para deserializar los bytes
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        // Identificador del grupo de consumidores al que pertenece
        props.put("group.id","grupo1");
        // nable.auto.commit = true -> El offset se escribirá periódicamente en segundo plano
        props.put("enable.auto.commit","true");
        // Indica la frecuencia en ms con la que se realiza esta operación (escribir offset)
        props.put("auto.commit.interval.ms","1000");
        // Indica la cantidad mínima de bytes que quiere recibir del broker cuando quiere leer registros
        // Si la cantidad es menor, el broker esperará a llegar al mínimo
        // Cuando se tiene mucho uso de CPU se recomienda incrementar este valor
        props.put("fetch.min.bytes","1");
        // Indica cuánto esperar en caso el broker no tengo suficientes datos, el valor por defecto es 500ms
        // Para bajar un poco la latencia, podemos reducir este valor
        props.put("fetch.max.wait.ms","500");
        // Controla al máximo número de bytes que se devolverá a su partición asignada al consumidor
        props.put("max.partition.fetch.bytes","1048576");
        // Controla el tiempo que un consumidor se considera vivo o funcional sin enviar un mensaje de estado a los brokers
        // Valor por defecto, 3 segundos
        props.put("session.timeout.ms","10000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("topic-test"));

            while (true){
                // Consumiendo los mensajes, indicamos el tiempo de espera si no hay nuevos mensajes en el cluster
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

                for (ConsumerRecord<String, String> record : records){
                    System.out.println("Topic: " + record.topic() + ", ");
                    System.out.println("Partition: " + record.partition() + ", ");
                    System.out.println("Key: " + record.key() + ", ");
                    System.out.println("Value: " + record.value() + ", ");
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
