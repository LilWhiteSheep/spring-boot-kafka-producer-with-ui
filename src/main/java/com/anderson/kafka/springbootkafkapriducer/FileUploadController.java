package com.anderson.kafka.springbootkafkapriducer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Controller
public class FileUploadController
{
    private final byte[] fileNameBytes = {0x00, 0x09};
    //private final byte[] fileContentBytes = {0x00, 0x10};
    private final byte[] finalBytes = {0x00, 0x11};

    static boolean uploadingFlag = false;

    @Autowired
    private KafkaTemplate<Integer, byte[]> kafkaTemplate;

    private static final String TOPIC_TEST = "fileTest";

    @GetMapping("/")
    public String listUploadedFiles(Model model) throws IOException
    {
        return "uploadForm";
    }

    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file,
                                   RedirectAttributes redirectAttributes)
    {

        while(uploadingFlag)
        {
            System.out.println(uploadingFlag);
            if(!uploadingFlag)
            {
                break;
            }
        }
        if(!uploadingFlag)
        {
            uploadingFlag = true;
        }

        //TestFileTransfer
        int messageNo = 1;

        //for splitting the file
        byte[] fileBuffer = new byte[102400];// in byte

        try
        {
            //Avro Start Here
            File avsc = new File("message.avsc");
            try
            {
                Schema schema = new Schema.Parser().parse(avsc);
                GenericRecord message1 = new GenericData.Record(schema);
                message1.put("name", file.getName());
                LocalDateTime currentTime = LocalDateTime.now();
                message1.put("time", currentTime.toString());
                message1.put("size", Long.toString(file.getSize()));
                Random rd = new Random();
                message1.put("jobid", rd.nextInt(99998) + 1);

                //Create avro metadata file
                File avroFile = new File("messages.avro");
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                dataFileWriter.create(schema, avroFile);
                dataFileWriter.append(message1);
                dataFileWriter.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            //transfer multipartfile to file
            String fileName = file.getOriginalFilename();
            String prefix = fileName.substring(0, fileName.lastIndexOf("."));
            String suffix = fileName.substring(fileName.lastIndexOf("."));
            String filePath = "D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\temp\\" + fileName;
            System.out.println(prefix);
            System.out.println(suffix);
            Thread.sleep(5000);
            File tempFile = new File(filePath);
            tempFile.mkdirs();
            file.transferTo(tempFile);

            //Throw all files to zip and return byte[]
            ArrayList<File> files = new ArrayList<>(2);
            files.add(tempFile);
            files.add(new File("messages.avro"));
            //Avro End here & zip all files together


            System.out.println("fileName : " + file.getOriginalFilename());

            //send file name
            String finalFileName = prefix + ".zip";
            byte[] fileNameInBytes = finalFileName.getBytes();
            //send fineNameByte to let consumer know producer is sending file name
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_TEST, messageNo, fileNameBytes));
            //System.out.println("fileNameByte : " + messageNo + ", value : " + Arrays.toString(fileNameBytes));
            messageNo++;
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_TEST, messageNo, fileNameInBytes));
            //System.out.println("fileNameInByte : " + messageNo + ", value : " + Arrays.toString(fileNameInBytes));
            messageNo++;
            ZipFilesToByte(files);
            File zipFile;
            zipFile = new File("D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\temp\\" + "temp.zip");
            InputStream inputStream = new FileInputStream(zipFile);

            //send file
            while (inputStream.read(fileBuffer) != -1)
            {
                kafkaTemplate.send(new ProducerRecord<>(TOPIC_TEST, messageNo, fileBuffer));
                messageNo++;
            }

            //after file transfer is done, delete tempFile
            if (tempFile.exists())
            {
                tempFile.delete();
            }

            if (zipFile.exists())
            {
                inputStream.close();
                zipFile.delete();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            //let consumer producer is done file transfer
            kafkaTemplate.send(new ProducerRecord<>(TOPIC_TEST, messageNo, finalBytes));
            uploadingFlag = false;
        }

        return "redirect:/";
    }

    private void ZipFilesToByte(ArrayList<File> files) throws IOException
    {
        byte[] fileBuffer = new byte[2048];
        File zipFile = new File("D:\\NtustMaster\\First\\Project\\CIMFORCE\\testFile\\temp\\" + "temp.zip");
        ZipOutputStream zipOutputStream = null;
        InputStream inputStream = null;
        zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile));


        for (File file : files)
        {
            //new zip entry and copying inputstream with file to zipOutputStream, after all closing streams
            zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
            inputStream = new FileInputStream(file);

            while(inputStream.read(fileBuffer) != -1)
            {
                zipOutputStream.write(fileBuffer);
            }
//            IOUtils.copyLarge(fileInputStream, zipOutputStream);
            inputStream.close();

        }

        zipOutputStream.close();
    }

}
