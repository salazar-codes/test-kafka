import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Productor {

    public static void main(String[] args) {

        // Definimos un objeto para almacenar propiedades del Producer
        Properties props = new Properties();

        // Definimos el serializer a utilizar por la clave-valor
        // Los Serializadores deben coincidir con los desearlizadores de los Consumers
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // acknowledgment -> Controla si el productor espera confirmación de escritura del broker o no
        // En caso de esperar también se define el nro de replicas que deben ser escritas antes de confirmar
        // Está relacionado con la pérdida de información
        // 0 -> no espera confirmación
        // 1 -> cuando la réplica lider escribe
        // all -> cuando todas las réplicas sincronizadas escriben el mensaje
        props.put("acks","all");

        // Servidores boostrap -> lista de hosts y puertos al que se conecta el Productor para conectarse inicialmente
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        // Reintentos en caso de fallo
        props.put("retries",0);
        // Tamaño del buffer que almacena eventos que pertenencen a la misma partición para enviarlos
        props.put("batch.size", 16384);
        // Controla el total de memoria disponible para el buffer del productor
        props.put("buffer.memory",33554432);


        KafkaProducer<String, String> prod = new KafkaProducer<>(props);
        String topic = "topic-test";
        int partition = 0;

        String key = "testKey";
        String value = "testValue";

        prod.send(new ProducerRecord<>(topic,partition,key,value));
        prod.close();
    }
}
