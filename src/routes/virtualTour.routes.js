// Backend/src/routes/virtualTour.routes.js
import { Router } from "express";
import { getHistoricalPlaces } from "../controllers/virtualTour.controller.js";

const router = Router();

router.get("/historical-places", getHistoricalPlaces);

export default router;