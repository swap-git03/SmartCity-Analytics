"""
UrbanPulse Decision Support & Recommendation Engine.

Generates actionable, rule-based mobility recommendations and environmental warnings
for Mumbai locations based on real-time data, ML predictions, and peak hour patterns.
"""

from typing import Dict, List, Any


class DecisionSupportEngine:
    """Rule-based decision support and traveler recommendation generator."""

    @staticmethod
    def generate_recommendations(
        location_name: str,
        current_speed: float,
        predicted_speed: float,
        free_flow_speed: float,
        current_aqi: int,
        predicted_aqi: float,
        weather_condition: str,
        hour: int
    ) -> Dict[str, Any]:
        """
        Generates actionable recommendations and mobility alerts for a specific Mumbai location.
        """
        recommendations: List[str] = []
        alerts: List[str] = []
        travel_window: str = "Optimal travel window: 11:00 AM - 1:30 PM & 9:00 PM - 11:00 PM"

        # 1. Congestion & Speed Warning Rules
        speed_drop_percent = round(((current_speed - predicted_speed) / max(current_speed, 1.0)) * 100, 1)
        if predicted_speed < 15.0 or speed_drop_percent > 20:
            alerts.append(f"⚠️ Heavy congestion expected in {location_name} during the next hour. (Predicted Speed: {predicted_speed} km/h)")
            recommendations.append(f"Consider alternative routes via Western/Eastern Express Highway to bypass {location_name}.")

        # 2. Peak Hour Rule
        if 8 <= hour <= 10 or 17 <= hour <= 20:
            alerts.append(f"🚨 Peak traffic rush hour active in {location_name}.")
            travel_window = "Recommended travel window: Post 8:30 PM or between 1:00 PM - 4:00 PM"
        else:
            recommendations.append(f"Traffic flow in {location_name} is currently near optimal speed.")

        # 3. AQI Environmental Alert Rule
        if predicted_aqi >= 3.5 or current_aqi >= 4:
            alerts.append(f"😷 Poor AQI predicted in {location_name} (AQI Level: {predicted_aqi}).")
            recommendations.append("Air quality alert: Sensitive groups should limit outdoor activity in this zone.")

        # 4. Weather Impact Rule
        if weather_condition.lower() in ["rain", "thunderstorm", "drizzle", "mist"]:
            alerts.append(f"🌧️ Adverse weather ({weather_condition}) likely to increase travel times in {location_name} by 25-40%.")

        return {
            "location": location_name,
            "primary_alert": alerts[0] if alerts else f"✅ Traffic and environmental metrics normal in {location_name}.",
            "alerts": alerts,
            "recommendations": recommendations,
            "recommended_travel_window": travel_window
        }
