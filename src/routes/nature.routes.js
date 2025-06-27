import { Router } from "express";
import { getNorthernDestinations } from "../controllers/nature.controller.js";

const router = Router();

router.get("/northern", getNorthernDestinations);

export default router;
