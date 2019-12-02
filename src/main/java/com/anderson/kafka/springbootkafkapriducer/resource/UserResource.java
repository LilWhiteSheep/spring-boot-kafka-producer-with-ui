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
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.time.*;
import java.util.Random;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.IOUtils;

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

    private static final String TOPIC = "Kafka_Example_file";//for File
    private static final String M_TOPIC = "MessageQueue";
    //private static final String TEST_TOPIC = "topicTest";

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
        //byte[] fileInByte = readFileToByte(file);
        //blockCount = (fileInByte.length / blockSize) + 1;

        //Avro Start Here
        File avsc = new File("message.avsc");
        try
        {
            Schema schema = new Schema.Parser().parse(avsc);
            GenericRecord message1 = new GenericData.Record(schema);
            message1.put("name", file.getName());
            LocalDateTime currentTime = LocalDateTime.now();
            message1.put("time", currentTime.toString());
            message1.put("size", Long.toString(file.length()));
            Random rd = new Random();
            message1.put("jobid", rd.nextInt(99998) + 1);

            //Create avro metadata file
            File avroFile = new File("messages.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, avroFile);
            dataFileWriter.append(message1);
            dataFileWriter.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        //Throw all files to zip and return byte[]
        ArrayList<File> files = new ArrayList<>(2);
        files.add(file);
        files.add(new File("messages.avro"));
        //Avro End here & zip all files together

        try
        {
            byte[] fileInByte = ZipFilesToByte(files);
            blockCount = (fileInByte.length / blockSize) + 1;

            for (int i = 0; i < blockCount - 1; i++)
            {
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
                Exception e)
        {
            e.printStackTrace();
        } finally
        {
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, finalBytes));
        }
        //TestFileTransferEnd


        //JsonVer
//        kafkaTemplate.send(TOPIC, new User(name, "Tech"));
        //JsonVerEnd

        return "Published successfully";
    }

    private byte[] readFileToByte(File file)
    {
        FileInputStream fileInputStream = null;

        byte[] bytesArray = new byte[(int) file.length()];
        try
        {
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);
            fileInputStream.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        //copy byte to String
//        String[] bytes = new String[bytesArray.length];
//        for (int i = 0; i < bytes.length; i++) {
//            bytes[i] = String.valueOf(bytesArray[i]);
//        }
        return bytesArray;
    }

    //ZIPPPPPPPPPPPPPPPPP
    private byte[] ZipFilesToByte(ArrayList<File> files) throws IOException
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream);
        ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream);

        for (File file : files)
        {
            //new zip entry and copying inputstream with file to zipOutputStream, after all closing streams
            zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
            FileInputStream fileInputStream = new FileInputStream(file);

            IOUtils.copy(fileInputStream, zipOutputStream);

            fileInputStream.close();
            zipOutputStream.closeEntry();
        }

        if (zipOutputStream != null)
        {
            zipOutputStream.finish();
            zipOutputStream.flush();
            zipOutputStream.close();
        }
        bufferedOutputStream.close();
        byteArrayOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }
}
