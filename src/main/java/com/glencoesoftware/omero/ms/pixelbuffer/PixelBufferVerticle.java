/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.ms.pixelbuffer;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import ome.model.annotations.FileAnnotation;
import ome.model.core.Image;
import ome.model.core.OriginalFile;
import ome.model.fs.Fileset;
import ome.model.fs.FilesetEntry;
import ome.io.nio.FileBuffer;
import ome.io.nio.OriginalFilesService;


/**
 * OMERO thumbnail provider worker verticle. This verticle is designed to be
 * deployed in worker mode and in either a single or multi threaded mode. It
 * acts as a pool of workers to handle blocking thumbnail rendering events
 * dispatched via the Vert.x EventBus.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelBufferVerticle extends AbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBufferVerticle.class);

    public static final String GET_TILE_EVENT =
            "omero.pixel_buffer.get_tile";

    public static final String GET_FILE_ANNOTATION_EVENT =
            "omero.pixel_buffer.get_file_annotation";

    public static final String GET_ORIGINAL_FILE_EVENT =
            "omero.pixel_buffer.get_original_file";

    public static final String GET_ZIPPED_FILES_EVENT =
            "omero.pixel_buffer.get_zipped_files";

    public static final String GET_ZIPPED_FILES_MULTIPLE_EVENT =
            "omero.pixel_buffer.get_zipped_files_multiple";

    private static final String GET_OBJECT_EVENT =
            "omero.get_object";

    public static final String GET_FILE_PATH_EVENT =
            "omero.get_file_path";

    public static final String GET_IMPORTED_IMAGE_FILES_EVENT =
            "omero.get_imported_image_files";

    public static final String GET_ORIGINAL_FILE_PATHS_EVENT =
            "omero.get_original_file_paths";

    /** OMERO server host */
    private final String host;

    /** OMERO server port */
    private final int port;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** Original File Service for getting paths */
    private OriginalFilesService ioService;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public PixelBufferVerticle(
            String host, int port, String omeroDataDir, ApplicationContext context) {
        this.host = host;
        this.port = port;
        this.context = context;
        this.ioService = new OriginalFilesService(omeroDataDir, true);
    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        log.info("Starting verticle");

        vertx.eventBus().<String>consumer(
                GET_TILE_EVENT, this::getTile);

        vertx.eventBus().<String>consumer(
                GET_FILE_ANNOTATION_EVENT, this::getFileAnnotation);

        vertx.eventBus().<JsonObject>consumer(
                GET_ORIGINAL_FILE_EVENT, this::getOriginalFile);

        vertx.eventBus().<JsonObject>consumer(
                GET_ZIPPED_FILES_EVENT, this::getZippedFiles);

        vertx.eventBus().<JsonObject>consumer(
                GET_ZIPPED_FILES_MULTIPLE_EVENT, this::getZippedFilesMultiple);
    }

    private void getTile(Message<String> message) {
        StopWatch t0 = new Slf4JStopWatch("getTile");
        ObjectMapper mapper = new ObjectMapper();
        TileCtx tileCtx;
        try {
            tileCtx = mapper.readValue(message.body(), TileCtx.class);
        } catch (Exception e) {
            String v = "Illegal tile context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            t0.stop();
            return;
        }
        log.debug("Load tile with data: {}", message.body());
        log.debug("Connecting to the server: {}, {}, {}",
                  host, port, tileCtx.omeroSessionKey);

        new TileRequestHandler(context, tileCtx, vertx).getTile(tileCtx.omeroSessionKey, tileCtx.imageId)
        .whenComplete(new BiConsumer<byte[], Throwable>() {
            @Override
            public void accept(byte[] tile, Throwable t) {
                if (t != null) {
                    if (t instanceof ReplyException) {
                        // Downstream event handling failure, propagate it
                        t0.stop();
                        message.fail(
                            ((ReplyException) t).failureCode(), t.getMessage());
                    } else {
                        String s = "Internal error";
                        log.error(s, t);
                        t0.stop();
                        message.fail(500, s);
                    }
                } else if (tile == null) {
                    message.fail(
                            404, "Cannot find Image:" + tileCtx.imageId);
                } else {
                    DeliveryOptions deliveryOptions = new DeliveryOptions();
                    deliveryOptions.addHeader(
                        "filename", String.format(
                            "image%d_z%d_c%d_t%d_x%d_y%d_w%d_h%d.%s",
                            tileCtx.imageId, tileCtx.z, tileCtx.c, tileCtx.t,
                            tileCtx.region.getX(),
                            tileCtx.region.getY(),
                            tileCtx.region.getWidth(),
                            tileCtx.region.getHeight(),
                            Optional.ofNullable(tileCtx.format).orElse("bin")
                        )
                    );
                    message.reply(tile, deliveryOptions);
                }
            };
        });
    }

    private <T> T deserialize(AsyncResult<Message<byte[]>> result)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais =
                new ByteArrayInputStream(result.result().body());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (T) ois.readObject();
    }

    private void getFileAnnotation(Message<String> message) {
        JsonObject messageBody = new JsonObject(message.body());
        String sessionKey = messageBody.getString("sessionKey");
        Long annotationId = messageBody.getLong("annotationId");
        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("type", "FileAnnotation");
        data.put("id", annotationId);
        vertx.eventBus().<byte[]>send(
                GET_OBJECT_EVENT, data, fileAnnotationResult -> {
            try {
                if (fileAnnotationResult.failed()) {
                    log.error(fileAnnotationResult.cause().getMessage());
                    message.reply("Failed to get annotation");
                    return;
                }
                FileAnnotation fileAnnotation = deserialize(fileAnnotationResult);
                Long fileId = fileAnnotation.getFile().getId();
                final JsonObject getOriginalFileData = new JsonObject();
                getOriginalFileData.put("sessionKey", sessionKey);
                getOriginalFileData.put("type", "OriginalFile");
                getOriginalFileData.put("id", fileId);
                log.info(getOriginalFileData.toString());
                vertx.eventBus().<byte[]>send(
                        GET_OBJECT_EVENT, getOriginalFileData, originalFileResult -> {
                    try {
                        if (originalFileResult.failed()) {
                            log.info(originalFileResult.cause().getMessage());
                            message.reply("Failed to get annotation");
                            return;
                        }
                        OriginalFile of = deserialize(originalFileResult);
                        FileBuffer fBuffer = ioService.getFileBuffer(of, "r");
                        log.info(fBuffer.getPath());
                        message.reply(fBuffer.getPath());
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Failed to get OriginalFile");
                        return;
                    }
                });
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                message.fail(404, "Failed to get FileAnnotation");
                return;
            }
        });
    }

    private void getOriginalFile(Message<JsonObject> message) {
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        Long fileId = messageBody.getLong("fileId");
        log.debug("Session key: " + sessionKey);
        log.debug("File ID: {}", fileId);

        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("id", fileId);
        data.put("type", "OriginalFile");
        vertx.eventBus().<byte[]>send(
                GET_FILE_PATH_EVENT, data, filePathResult -> {
            try {
                if (filePathResult.failed()) {
                    log.error(filePathResult.cause().getMessage());
                    message.fail(404, "Failed to get file path");
                    return;
                }
                final String filePath = deserialize(filePathResult);
                vertx.eventBus().<byte[]>send(
                        GET_OBJECT_EVENT, data, getOriginalFileResult -> {
                    try {
                        if (getOriginalFileResult.failed()) {
                            log.error(getOriginalFileResult.cause().getMessage());
                            message.fail(404, "Failed to get original file");
                            return;
                        }
                        OriginalFile of = deserialize(getOriginalFileResult);
                        String fileName = of.getName();
                        String mimeType = of.getMimetype();
                        log.info(fileName);
                        log.info(mimeType);
                        JsonObject response = new JsonObject();
                        response.put("filePath", filePath);
                        response.put("fileName", fileName);
                        response.put("mimeType", mimeType);
                        message.reply(response);
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Error decoding object");
                    }
                });
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                message.fail(404, "Error decoding file path object");
            }
        });
    }

    private boolean createZip(String fullZipPath, List<String> filePaths) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(fullZipPath);
        ZipOutputStream zos = new ZipOutputStream(fos);
        try {
        for (String fpath : filePaths) {
                File f = new File(fpath);
                ZipEntry entry = new ZipEntry(f.getName());
                zos.putNextEntry(entry);
                FileInputStream fis = new FileInputStream(f);

                byte[] bytes = new byte[1024];
                int length;
                while((length = fis.read(bytes)) >= 0) {
                    zos.write(bytes, 0, length);
                }
                fis.close();
        }
        zos.close();
        fos.close();
        } catch (IOException e) {
            log.error("Failure during zip: " + e.getMessage());
            return false;
        }
        return true;
    }

    private void getZippedFiles(Message<JsonObject> message) {
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        Long imageId = messageBody.getLong("imageId");
        final String zipDirectory = messageBody.getString("zipDirectory");
        log.debug("Session key: " + sessionKey);
        log.debug("Image ID: {}", imageId);

        //Get OriginalFiles for Image
        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("imageId", imageId);
        vertx.eventBus().<byte[]>send(GET_IMPORTED_IMAGE_FILES_EVENT, data, getImportedFilesResult -> {
            try {
                if (getImportedFilesResult.failed()) {
                    log.error(getImportedFilesResult.cause().getMessage());
                    message.reply("Failed to get image");
                    return;
                }
                List<OriginalFile> originalFiles = deserialize(getImportedFilesResult);
                log.info(String.valueOf(originalFiles.size()));
                JsonArray fileIds = new JsonArray();
                for (OriginalFile of : originalFiles) {
                    fileIds.add(of.getId());
                }
                final JsonObject filepathData = new JsonObject();
                filepathData.put("sessionKey", sessionKey);
                filepathData.put("originalFileIds", fileIds);
                //Get File Paths for OriginalFiles
                log.info("Getting OriginalFile Paths");
                vertx.eventBus().<byte[]>send(
                        GET_ORIGINAL_FILE_PATHS_EVENT, filepathData, filePathResult -> {
                    try {
                        if (filePathResult.failed()) {
                            log.error(filePathResult.cause().getMessage());
                            message.fail(404, "Failed to get file path");
                            return;
                        }
                        final JsonObject filePathResultObj = deserialize(filePathResult);
                        final JsonArray filePathArray = filePathResultObj.getJsonArray("paths");
                        final String managedRepositoryRoot = filePathResultObj.getString("managedRepositoryRoot");
                        final List<String> filePaths = new ArrayList<String>(filePathArray.size());
                        for(int i = 0; i < filePathArray.size(); i++) {
                            filePaths.add(filePathArray.getString(i));
                        }
                        String zipName = "image" + imageId.toString() + ".zip";
                        String zipFullPath = zipDirectory + File.separator + zipName;
                        File zipFile = new File(zipFullPath);
                        int fileIndex = 1;
                        while (zipFile.exists()) {
                            log.info("Zip name collision: " + zipFullPath);
                            zipName = "image" + imageId.toString()
                                       + "_" + String.valueOf(fileIndex) + ".zip";
                            zipFullPath = zipDirectory + File.separator + zipName;
                            zipFile = new File(zipFullPath);
                        }
                        boolean success = createZip(zipFullPath, filePaths);
                        if (success) {
                            JsonObject pathObj = new JsonObject();
                            pathObj.put("filePath", zipFullPath);
                            pathObj.put("fileName", zipName);
                            pathObj.put("mimeType", "application/zip");
                            message.reply(pathObj);
                        }
                        else {
                            message.fail(404, "Error creating zip");
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Error decoding file path object");
                    }
                });
            } catch (IOException | ClassNotFoundException e) {

            }
        });
    }

    public static boolean createZipDir(String tempZipDir,
            String zipName) {
        try {
            FileOutputStream fos = new FileOutputStream(zipName);
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            File fileToZip = new File(tempZipDir);

            zipFile(fileToZip, fileToZip.getName(), zipOut);
            zipOut.close();
            fos.close();
            return true;
        } catch (IOException e) {
            log.error(e.getMessage());
            return false;
        }
    }

    private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith(File.separator)) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + File.separator));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            for (File childFile : children) {
                zipFile(childFile, fileName + File.separator + childFile.getName(), zipOut);
            }
            return;
        }
        FileInputStream fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        fis.close();
    }

    private String getTargetPath(String filePath,
                                 String fileName,
                                 String templatePrefix,
                                 String managedRepositoryRoot) {
        Path rootPath = Paths.get(managedRepositoryRoot, templatePrefix);
        Path fpath = Paths.get(filePath);
        Path parent = fpath.getParent();
        String parentStr = parent.toString();
        if (parentStr.equals(rootPath.toString()) || templatePrefix.equals("")) {
            return fileName;
        }
        String relPath = rootPath.toUri().relativize(new File(parentStr).toURI()).getPath();
        Path path = Paths.get(relPath);
        return path.resolve(fileName).toString();
    }

    private void getZippedFilesHelper(Message<JsonObject> message,
            Deque<Long> remainingImageIds,
            String tempZipDir,
            String zipName,
            Set<Long> usedFilesetIds,
            Set<Long> usedFileIds) {
        if(remainingImageIds.size() == 0) {
            //Zip the files and respond to the message
            boolean success = createZipDir(tempZipDir, zipName);
            if (success) {
                JsonObject pathObj = new JsonObject();
                pathObj.put("zipName", zipName);
                message.reply(pathObj);
            }
            else {
                message.fail(404, "Error creating zip");
            }
            return;
        }

        Long imageId = remainingImageIds.pop();
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        //Get the image
        final JsonObject getImageData = new JsonObject();
        getImageData.put("sessionKey", sessionKey);
        getImageData.put("id", imageId);
        getImageData.put("type", "Image");
        vertx.eventBus().<byte[]>send(GET_OBJECT_EVENT, getImageData, getImageResult -> {
            try {
                if (getImageResult.failed()) {
                    log.error(getImageResult.cause().getMessage());
                    message.reply("Failed to get image");
                    return;
                }
                Image thisImage = deserialize(getImageResult);
                Long fsId = thisImage.getFileset().getId();
                //If we've already processed this fileset, move on
                if (usedFilesetIds.contains(fsId)) {
                    getZippedFilesHelper(message,
                            remainingImageIds,
                            tempZipDir,
                            zipName,
                            usedFilesetIds,
                            usedFileIds);
                    return;
                }
                //Get the fileset
                final JsonObject getFilesetData = new JsonObject();
                getFilesetData.put("sessionKey", sessionKey);
                getFilesetData.put("id", fsId);
                getFilesetData.put("type", "Fileset");
                vertx.eventBus().<byte[]>send(GET_OBJECT_EVENT, getFilesetData, getFilesetResult -> {
                    try {
                        if (getFilesetResult.failed()) {
                            log.error(getFilesetResult.cause().getMessage());
                            message.reply("Failed to get image");
                            return;
                        }
                        Fileset thisFileset = deserialize(getFilesetResult);
                        String templatePrefix = thisFileset.getTemplatePrefix();
                        //Get OriginalFiles for Image
                        final JsonObject getImportedImageFilesData = new JsonObject();
                        getImportedImageFilesData.put("sessionKey", sessionKey);
                        getImportedImageFilesData.put("imageId", imageId);
                        vertx.eventBus().<byte[]>send(GET_IMPORTED_IMAGE_FILES_EVENT, getImportedImageFilesData, getImportedFilesResult -> {
                            try {
                                if (getImportedFilesResult.failed()) {
                                    log.error(getImportedFilesResult.cause().getMessage());
                                    message.reply("Failed to get image");
                                    return;
                                }
                                List<OriginalFile> originalFiles = deserialize(getImportedFilesResult);
                                log.info(String.valueOf(originalFiles.size()));
                                JsonArray fileIds = new JsonArray();
                                for (OriginalFile of : originalFiles) {
                                    fileIds.add(of.getId());
                                }
                                final JsonObject filepathData = new JsonObject();
                                filepathData.put("sessionKey", sessionKey);
                                filepathData.put("originalFileIds", fileIds);
                                //Get File Paths for OriginalFiles
                                log.info("Getting OriginalFile Paths");
                                vertx.eventBus().<byte[]>send(
                                        GET_ORIGINAL_FILE_PATHS_EVENT, filepathData, filePathResult -> {
                                    try {
                                        if (filePathResult.failed()) {
                                            log.error(filePathResult.cause().getMessage());
                                            message.fail(404, "Failed to get file path");
                                            return;
                                        }
                                        String newDir = "";
                                        String jsonString = deserialize(filePathResult);
                                        final JsonObject filePathResultObj = new JsonObject(jsonString);
                                        final JsonArray filePathArray = filePathResultObj.getJsonArray("paths");
                                        final String managedRepositoryRoot = filePathResultObj.getString("managedRepositoryRoot");
                                        log.info("Managed Repo Root: " + managedRepositoryRoot);
                                        final List<String> filePaths = new ArrayList<String>(filePathArray.size());
                                        for(int i = 0; i < filePathArray.size(); i++) {
                                            filePaths.add(filePathArray.getString(i));
                                        }
                                        for (String fpath : filePaths) {
                                            String fileName = fpath.split(File.separator)[fpath.split(File.separator).length - 1];
                                            String targetPath = getTargetPath(fpath,
                                                                              fileName,
                                                                              templatePrefix,
                                                                              managedRepositoryRoot);
                                            File baseFile = new File(tempZipDir
                                                            + File.separator
                                                            + targetPath.split(File.separator)[0]);
                                            log.info(baseFile.toString());
                                            if (baseFile.exists()){
                                                Integer newDirIdx = 0;
                                                File baseTest = new File(tempZipDir
                                                        + File.separator
                                                        + newDirIdx.toString()
                                                        + File.separator
                                                        + targetPath.split(File.separator)[0]);
                                                while(baseTest.exists()) {
                                                    newDirIdx += 1;
                                                    baseTest = new File(tempZipDir
                                                            + File.separator
                                                            + newDirIdx.toString()
                                                            + File.separator
                                                            + targetPath.split(File.separator)[0]);
                                                }
                                                newDir = newDirIdx.toString();
                                                break;
                                            }
                                        }

                                        //Copy each file into the temp zip dir
                                        String newZipName = zipName;
                                        for (String fpath : filePaths) {
                                            //TODO: Check for duplicate Omero 4.4 images
                                            String fileName = fpath.split(File.separator)[fpath.split(File.separator).length - 1];
                                            String temp_f = getTargetPath(fpath, fileName, templatePrefix, managedRepositoryRoot);
                                            Path tempfPath = Paths.get(tempZipDir, newDir, temp_f);
                                            File temp_d = new File(tempfPath.getParent().toString());
                                            if (!temp_d.exists()) {
                                                temp_d.mkdirs();
                                            }
                                            /*
                                             * Need to be sure that the zip name does not match any file
                                             * within it since OS X will unzip as a single file instead
                                             * of a directory
                                             */
                                            if (newZipName == fileName + ".zip") {
                                                newZipName = fileName + "_folder.zip";
                                            }

                                            Path originalPath = Paths.get(fpath);
                                            Files.copy(originalPath, tempfPath, StandardCopyOption.REPLACE_EXISTING);
                                        }

                                        getZippedFilesHelper(message,
                                            remainingImageIds,
                                            tempZipDir,
                                            newZipName,
                                            usedFilesetIds,
                                            usedFileIds);
                                    } catch (IOException | ClassNotFoundException e) {
                                        log.error("Exception while decoding object in response", e);
                                        message.fail(404, "Error decoding file path object");
                                    }
                                });
                            } catch (IOException | ClassNotFoundException e) {

                            }
                        });
                    } catch (IOException | ClassNotFoundException e) {

                    }
                });
            } catch (IOException | ClassNotFoundException e) {

            }
        });

    }

    private void getZippedFilesMultiple(Message<JsonObject> message) {
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        String zipDirectory = messageBody.getString("zipDirectory");
        JsonArray imageIdArray = messageBody.getJsonArray("imageIds");
        Deque<Long> remainingImageIds = new ArrayDeque<Long>(imageIdArray.size());
        for(int i = 0; i < imageIdArray.size(); i++) {
            remainingImageIds.add(imageIdArray.getLong(i));
        }

        log.info(remainingImageIds.toString());

        String tempZipDir = zipDirectory + File.separator + "testTmpZipDir";
        String zipName = "testZip.zip";
        Set<Long> usedFilesetIds = new HashSet<Long>();
        Set<Long> usedFileIds = new HashSet<Long>();

        getZippedFilesHelper(message,
                remainingImageIds,
                tempZipDir,
                zipName,
                usedFilesetIds,
                usedFileIds);
    }

}
