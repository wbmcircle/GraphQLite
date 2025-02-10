import Postgres from "db/postgres";
import express from "express";
import { Op } from 'sequelize';

export const getMessages = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { senderId, chatId, isDeleted } = req.query;

    if (!senderId || !chatId) {
      throw new Error("Missing required query parameters: senderId, chatId");
    }

    const messages = await Postgres.models.messages.findAll({
      where: {
        senderId,
        chatId,
        isDeleted: false
      },
      attributes: ['chatId', 'chatType', 'objectId', 'isDeleted', 'senderId', 'createdAt'],
      order: [["createdAt", "ASC"]],
    });

    res.locals.data = messages;
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const getChannelMessages = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { senderId, chatId, isDeleted } = req.query;

    if (!senderId || !chatId) {
      throw new Error("Missing required query parameters: senderId, chatId");
    }

    const messages = await Postgres.models.messages.findAll({
      where: {
        chatId,
        isDeleted: false
      },
      attributes: ['chatId', 'chatType', 'objectId', 'isDeleted', 'senderId'],
      order: [["createdAt", "ASC"]],
    });

    res.locals.data = messages;
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const getCallMessages = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const messages = await Postgres.models.messages.findAll({
      where: {
        isDeleted: false,
        type: 'text', 
        text: {
          [Op.like]: '%[Jitsi_Call_Log:]%' 
        }
      },
      attributes: ['chatId', 'chatType', 'objectId', 'isDeleted', 'senderId', 'createdAt', 'text'],
      order: [["createdAt", "DESC"]],
    });

    res.locals.data = messages;
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const getCallMessagesByChatId = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { chatId } = req.query;

    const messages = await Postgres.models.messages.findAll({
      where: {
        chatId,
        isDeleted: false,
        type: 'text', 
        text: {
          [Op.like]: '%[Jitsi_Call_Log:]%' 
        }
      },
      attributes: ['chatId', 'chatType', 'objectId', 'isDeleted', 'senderId', 'createdAt', 'text'],
      order: [["createdAt", "DESC"]],
    });

    res.locals.data = messages;
    return next("router");
  } catch (err) {
    return next(err);
  }
};

export const updateMessages = async (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  try {
    const { chatId, senderId, objectId } = req.body;
    console.log('chatId', chatId);
    console.log('senderId', senderId);
    console.log('objectId', objectId);

    if (!senderId || !objectId || !chatId) {
      throw new Error("Missing required parameters: senderId, objectId, chatId");
    }

    await Postgres.models.messages.update(
      { isDeleted: true },
      {
        where: {
          senderId,
          objectId,
          chatId,
        },
      }
    );

    res.locals.data = { message: "Message updated successfully" };
    return next("router");
  } catch (err) {
    return next(err);
  }
};