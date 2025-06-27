import { Router } from "express";
import { callGeminiAPI } from "../controllers/chat.controller.js";

const router = Router();


router.post("/ask", callGeminiAPI);

export default router;