package com.anderson.kafka.springbootkafkapriducer;

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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;

@Controller
public class FileUploadController {

//    private final StorageService storageService;

//    @Autowired
//    public FileUploadController(StorageService storageService) {
//        this.storageService = storageService;
//    }

//    @Autowired
//    private MqttfilesRepository mqttfilesRepository;

    private final byte[] finalBytes = {0x00, 0x11};

    @Autowired
    private KafkaTemplate<Integer, byte[]> kafkaTemplate;

    private static final String TOPIC = "Kafka_Example_file";

    @GetMapping("/")
    public String listUploadedFiles(Model model) throws IOException {

//        model.addAttribute("files", storageService.loadAll().map(
//                path -> MvcUriComponentsBuilder.fromMethodName(FileUploadController.class,
//                        "serveFile", path.getFileName().toString()).build().toString())
//                .collect(Collectors.toList()));

        return "uploadForm";
    }

//    @GetMapping("/files/{filename:.+}")
//    @ResponseBody
//    public ResponseEntity<Resource> serveFile(@PathVariable String filename) {
//
//        Resource file = storageService.loadAsResource(filename);
//        return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
//                "attachment; filename=\"" + file.getFilename() + "\"").body(file);
//    }

    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file,
                                   RedirectAttributes redirectAttributes) {
        //TestFileTransfer
        int messageNo = 1;

        //for splitting
        int blockSize = 2048;// in byte
        int blockCount;
        byte[] byteChunk = null;

        try {
            byte[] fileInByte = file.getBytes();
            blockCount = (fileInByte.length / blockSize) + 1;

            for (int i = 0; i < blockCount - 1; i++) {
                int pointer = i * blockSize;
                byteChunk = Arrays.copyOfRange(fileInByte, pointer, pointer + blockSize);
                kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, byteChunk));
                messageNo++;
            }
            byteChunk = Arrays.copyOfRange(fileInByte, blockSize * (blockCount - 1), fileInByte.length);
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, byteChunk));

            messageNo++;
//            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, finalBytes));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            kafkaTemplate.send(new ProducerRecord<>(TOPIC, messageNo, finalBytes));
        }


//        storageService.store(file);
//
//        Mqttfiles mq = new Mqttfiles();
//
//        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//        mq.setCreateAt(timestamp);
//
//        mq.setCreator(5);
//        mq.setFilepath( file.getOriginalFilename() );
//        mq.setState(0);
//
//        redirectAttributes.addFlashAttribute("message",
//                "You successfully uploaded " + file.getOriginalFilename() + "!");

        return "redirect:/";
    }

}
