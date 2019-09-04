package com.anderson.kafka.springbootkafkapriducer.resource;

import com.anderson.kafka.springbootkafkapriducer.model.User;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

@RestController
@RequestMapping("kafka")
public class UserResource
{
    private final byte[] finalBytes = {0x00, 0x11};

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
    //JsonVer
//    @Autowired
//    private KafkaTemplate<String, User> kafkaTemplate;
    //JsonVerEnd
    //TestFileTransfer
    @Autowired
    private KafkaTemplate<Integer, byte[]> kafkaTemplate;
    //TestFileTransferEnd


//    private static final String TOPIC = "Kafka_Example";
//    private static final String TOPIC = "Kafka_Example_json";

    private static final String TOPIC = "Kafka_Example_file";

    //    @GetMapping("/publish/{message}")
//    public String post(@PathVariable("message") final String message)
//    {
//
//        kafkaTemplate.send(TOPIC, message);
//
//        return "Published successfully";
//    }
    @GetMapping("/publish/{name}")
    public String post(@PathVariable("name") final String name)
    {

        //TestFileTransfer
        int messageNo = 1;

        //for splitting
        int blockSize = 2048;// in byte
        int blockCount;
        byte[] byteChunk = null;

        File file = new File("D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\test_6MB.pdf");
        byte[] fileInByte = readFileToByte(file);
        blockCount = (fileInByte.length / blockSize) + 1;

        try {
            for (int i = 0; i < blockCount - 1; i++) {
                int pointer = i * blockSize;
                byteChunk = Arrays.copyOfRange(fileInByte, pointer, pointer + blockSize);
                kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, byteChunk));
                messageNo++;
            }
            byteChunk = Arrays.copyOfRange(fileInByte, blockSize * (blockCount - 1), fileInByte.length);
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, byteChunk));

            messageNo++;
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, finalBytes));


        } catch (
                Exception e) {
            e.printStackTrace();
        } finally {
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, finalBytes));
        }
        //TestFileTransferEnd


        //JsonVer
//        kafkaTemplate.send(TOPIC, new User(name, "Tech"));
        //JsonVerEnd

        return "Published successfully";
    }

    private byte[] readFileToByte(File file) {
        FileInputStream fileInputStream = null;

        byte[] bytesArray = new byte[(int) file.length()];
        try {
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //copy byte to String
//        String[] bytes = new String[bytesArray.length];
//        for (int i = 0; i < bytes.length; i++) {
//            bytes[i] = String.valueOf(bytesArray[i]);
//        }
        return bytesArray;
    }
}
