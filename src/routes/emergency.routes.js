import { getNearbyPlaces } from "../controllers/emergency.controller.js";

import { Router } from "express";

export const router = Router();

router.post("/nearby", getNearbyPlaces);
export default router; 