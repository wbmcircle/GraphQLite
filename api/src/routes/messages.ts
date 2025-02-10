import * as messages from "../controllers/messages";
import { Router } from "express";
import authMiddleware from "middlewares/auth";

const router = Router();

router.get("/getMessages", authMiddleware, messages.getMessages);
router.get("/getChannelMessages", authMiddleware, messages.getChannelMessages);
router.get("/getCallMessages", authMiddleware, messages.getCallMessages);
router.get("/getCallMessagesByChatId", authMiddleware, messages.getCallMessagesByChatId);
router.post("/updateMessages", authMiddleware, messages.updateMessages);

export default router;