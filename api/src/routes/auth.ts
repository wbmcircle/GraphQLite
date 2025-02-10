import * as auth from "controllers/auth";
import { Router } from "express";
import authMiddleware from "middlewares/auth";

const router = Router();

router.get("/verify", authMiddleware, auth.verify);
router.post("/users", auth.create);
router.post("/users/:id", authMiddleware, auth.update);
router.post("/users/restore/:id", authMiddleware, auth.restoreUser);
router.delete("/users/:id", authMiddleware, auth.remove);
router.post("/login", auth.login);
router.post("/logout", authMiddleware, auth.logout);
router.post("/refresh", auth.refresh);
router.post("/workspace", auth.getWorkspace);
router.post("/getUser", auth.getUser);
router.post("/getDeletedUser", auth.getDeletedUser);
router.post("/getFcmToken", auth.getFcmToken);

export default router;
