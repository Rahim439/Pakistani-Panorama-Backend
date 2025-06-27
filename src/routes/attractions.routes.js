import { Router } from "express";
import {
  getHotels,
  getRestaurants,
  getAmusementParks,
} from "../controllers/attractions.controller.js";

const router = Router();

router.get("/hotels", getHotels);
router.get("/restaurants", getRestaurants);
router.get("/amusement-parks", getAmusementParks);

export default router;
