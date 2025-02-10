import ffmpegInstaller from "@ffmpeg-installer/ffmpeg";
import AWS from "aws-sdk";
import axios from "axios";
import express from "express";
import ffmpeg from "fluent-ffmpeg";
import fs from "fs";
import path from "path";
import { getHeadObject } from "utils/get-head";
import randomId from "utils/random-id";

ffmpeg.setFfmpegPath(ffmpegInstaller.path);

let downloadFile = async (fileUrl: any, outputLocationPath: any) => {
  const writer = fs.createWriteStream(outputLocationPath);

  return axios({
    method: 'get',
    url: fileUrl,
    responseType: 'stream',
  }).then(response => {

    //ensure that the user can call `then()` only when the file has
    //been downloaded entirely.

    return new Promise((resolve, reject) => {
      response.data.pipe(writer);
      let error: any = null;
      writer.on('error', err => {
        error = err;
        writer.close();
        reject(err);
      });
      writer.on('close', () => {
        if (!error) {
          resolve(true);
        }
        //no need to call the reject here, as it will have been called in the
        //'error' stream;
      });
    });
  });
}

let convert_video = async (inputPath: any, outputPath: any) => {
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .output(outputPath)
      .format('mp4') // Specify the output format as mp4
      .videoCodec('libx264')
      .on('end', () => {
        console.log('Conversion finished successfully.');
        resolve(true);
      })
      .on('error', (err) => {
        console.error('Error during conversion:', err.message);
        reject(err);
      })
      .run();
  });
}

let convert_audio = async (inputPath: any, outputPath: any) => {
  console.log('inputPath', inputPath)
  console.log('outputPath', outputPath)
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .output(outputPath)
      .toFormat('mp3')
      .on('end', () => {
        console.log('Audio conversion finished successfully.');
        resolve(true);
      })
      .on('error', (err) => {
        console.log('Error during conversion:', err.message);
        reject(err);
      })
      .run();
  });
}

export const s3 = new AWS.S3({
  accessKeyId: process.env.MINIO_ROOT_USER || "minioadmin",
  secretAccessKey: process.env.MINIO_ROOT_PASSWORD || "minioadmin",
  endpoint: "http://localhost:9000",
  s3ForcePathStyle: true,
  signatureVersion: "v4",
});

export const createBucket = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { name } = req.body;

    await s3.createBucket({ Bucket: name }).promise();

    res.locals.data = {
      bucketName: name,
    };
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const deleteBucket = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { name } = req.params;

    await s3.deleteBucket({ Bucket: name }).promise();

    res.locals.data = {
      success: true,
    };
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const listBuckets = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const buckets = await s3.listBuckets().promise();

    res.locals.data = {
      data: buckets.Buckets,
    };
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const uploadObject = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {

    const { name } = req.params;
    const { key } = req.body;

    if (!req.files) throw new Error("No files were uploaded.");
    const file = req.files.file as any;

    console.log('file name', file.name, file.mimetype)

    const fileToken = randomId(100);

    await s3
      .upload({
        Bucket: name,
        Key: key,
        Body: file.data,
        ContentType: file.mimetype,
        Metadata: {
          token: fileToken,
        },
      })
      .promise();

    if (file.name.endsWith('.m4a') || file.name.endsWith('.MOV')) {
      try {

        await downloadFile(`http://localhost:4000/storage/b/${name}/o/${encodeURIComponent(key)}?token=${fileToken}`, `${path.resolve(__dirname, "../../public")}/${file.name}`)

        let new_key = `${new Date().getTime()}`;
        const inputPath = `${path.resolve(__dirname, "../../public")}/${file.name}`;
        if (file.name.endsWith('.m4a')) {
          const outputPath = `${path.resolve(__dirname, "../../public")}/${new_key}.mp3`;
          await convert_audio(inputPath, outputPath)
  
          const readableStream = fs.createReadStream(outputPath)
          await s3
            .upload({
              Bucket: name,
              Key: `${new_key}.mp3`,
              Body: readableStream,
              ContentType: 'audio/mpeg',
              Metadata: {
                token: fileToken,
              },
            })
            .promise();
  
          res.locals.data = {
            url: `/storage/b/${name}/o/${encodeURIComponent(`${new_key}.mp3`)}?token=${fileToken}`,
          };
        } else {
          const outputPath = `${path.resolve(__dirname, "../../public")}/${new_key}.pm4`;
          await convert_video(inputPath, outputPath)
  
          const readableStream = fs.createReadStream(outputPath)
          await s3
            .upload({
              Bucket: name,
              Key: `${new_key}.mp4`,
              Body: readableStream,
              ContentType: 'video/mp4',
              Metadata: {
                token: fileToken,
              },
            })
            .promise();
  
          res.locals.data = {
            url: `/storage/b/${name}/o/${encodeURIComponent(`${new_key}.mp4`)}?token=${fileToken}`,
          };
        }
      }
      catch (err) {
        console.log('converting error', err);
        res.locals.data = {
          url: `/storage/b/${name}/o/${encodeURIComponent(key)}?token=${fileToken}`,
        };
      }
    } else {
      res.locals.data = {
        url: `/storage/b/${name}/o/${encodeURIComponent(key)}?token=${fileToken}`,
      };
    }

    return next("router");

    // const destFolder = './public/';

    // await file.mv(destFolder + file.name, function(err) {
    //   if (err)
    //     return res.status(500).send(err);

    //   res.locals.data = {
    //     url: `/${file.name}`,
    //   };
    //   return next("router");
    // });
  } catch (err) {
    console.log('err', err)
    return next(err);
  }
};

export const getObject = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { name, key } = req.params;

    if (!req.query.token)
      throw new Error("You are not authorized to download this file.");

    const headObject = await getHeadObject(s3, name, key);

    if (headObject.Metadata?.token !== req.query.token)
      throw new Error("You are not authorized to download this file.");

    const params = {
      Bucket: name,
      Key: key,
    };
    // Split request with stream to be able to abort request on timeout errors
    const request = s3.getObject(params);
    const stream = request.createReadStream().on("error", (err) => {
      console.error(err);
      request.abort();
    });

    const downloadName = req.query.d && typeof req.query.d === "string" ? req.query.d : key;
    const headerValue: string = `attachment; filename="${encodeURIComponent(downloadName)}"`;
    // Add the content type to the response (it's not propagated from the S3 SDK)
    res.set("Content-Type", headObject.ContentType);
    res.set("Content-Length", headObject.ContentLength?.toString());
    res.set("Last-Modified", headObject.LastModified?.toUTCString());
    res.set("Content-Disposition", headerValue);
    res.set("Cache-Control", "private,max-age=31557600");
    res.set("ETag", headObject.ETag);

    // Pipe the s3 object to the response
    stream.pipe(res);
  } catch (err) {
    return next(err);
  }
};

export const deleteObject = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { name, key } = req.params;

    await s3
      .deleteObject({
        Bucket: name,
        Key: key,
      })
      .promise();

    res.locals.data = {
      success: true,
    };
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const listObjects = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { name } = req.params;

    const objects = await s3.listObjects({ Bucket: name }).promise();

    res.locals.data = {
      data: objects.Contents
        ? await Promise.all(
          objects.Contents.map(async (o) => ({
            ...o,
            Head: await getHeadObject(s3, name, o.Key as string),
          }))
        )
        : [],
    };
    return next("router");
  } catch (err) {
    return next(err);
  }
};

