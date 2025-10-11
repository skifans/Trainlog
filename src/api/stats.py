import logging
from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, abort

from src.pg import pg_session
from src.sql import stats as stats_sql
from src.utils import (
    getUser,
    isCurrentTrip,
    lang,
    listOperatorsLogos,
    get_user_id
)

logger = logging.getLogger(__name__)

stats_blueprint = Blueprint("stats", __name__)

import json

def get_stats_countries(pg, user_id, trip_type, year=None):
    """Process country statistics with past/future breakdown"""
    result = pg.execute(
        stats_sql.stats_countries(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    countries = {}
    
    for row in result:
        row_dict = dict(row._mapping)
        country_list = row_dict['countries']
        
        try:
            country_dict = json.loads(country_list)
        except (json.JSONDecodeError, TypeError):
            continue
        
        for country in country_dict:
            if country not in countries:
                countries[country] = {
                    "totalTrips": 0,
                    "pastTrips": 0,
                    "plannedFutureTrips": 0,
                    "totalKm": 0,
                    "pastKm": 0,
                    "plannedFutureKm": 0
                }
            
            if isinstance(country_dict[country], dict):
                country_value = sum(country_dict[country].values())
            else:
                country_value = country_dict[country]
            
            # Add to km totals
            countries[country]["totalKm"] += country_value
            if row_dict["past"] != 0:
                countries[country]["pastKm"] += country_value
            elif row_dict["plannedFuture"] != 0:
                countries[country]["plannedFutureKm"] += country_value
            
            # Add to trip counts
            countries[country]["totalTrips"] += (
                row_dict["past"] + row_dict["plannedFuture"]
            )
            if row_dict["past"] != 0:
                countries[country]["pastTrips"] += row_dict["past"]
            elif row_dict["plannedFuture"] != 0:
                countries[country]["plannedFutureTrips"] += row_dict["plannedFuture"]
    
    # Sort by total trips descending
    countries = dict(
        sorted(
            countries.items(),
            key=lambda item: countries[item[0]]["totalTrips"],
            reverse=True,
        )
    )
    
    # Convert to list format
    countries_list = []
    for country in countries:
        countries_list.append(
            {
                "country": country,
                "pastTrips": countries[country]["pastTrips"],
                "plannedFutureTrips": countries[country]["plannedFutureTrips"],
                "pastKm": countries[country]["pastKm"],
                "plannedFutureKm": countries[country]["plannedFutureKm"],
            }
        )
    
    return countries_list


def get_stats_years(pg, user_id, lang, trip_type, year=None):
    """Process year statistics with gap filling"""
    years = []
    years_temp = {}
    future = None
    
    result = pg.execute(
        stats_sql.stats_year(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()

    if len(result) == 0:
        return ""
    
    result_list = [dict(row._mapping) for row in result]
    
    # Separate future from regular years
    future = next((y for y in result_list if y["year"] == "future"), None)
    result_list = [y for y in result_list if y["year"] != "future"]
    
    if len(result_list) == 0:
        if future:
            return [{
                "year": lang["future"],
                "pastTrips": 0,
                "plannedFutureTrips": 0,
                "futureTrips": future["futuretrips"],
                "pastKm": 0,
                "plannedFutureKm": 0,
                "futureKm": future["futurekm"],
            }]
        return ""
    
    # Build temp dictionary
    for year_row in result_list:
        years_temp[int(year_row["year"])] = {
            "pastTrips": int(year_row["pastTrips"]),
            "plannedFutureTrips": int(year_row["plannedFutureTrips"]),
            "futureTrips": int(year_row["futureTrips"]),
            "pastKm": int(year_row["pastKm"]),
            "plannedFutureKm": int(year_row["plannedFutureKm"]),
            "futureKm": int(year_row["futureKm"]),
        }
    
    # Fill gaps between first and last year
    first_year = int(result_list[0]["year"])
    last_year = int(result_list[-1]["year"])
    
    for year_num in range(first_year, last_year + 1):
        if year_num in years_temp:
            years.append(
                {
                    "year": year_num,
                    "pastTrips": years_temp[year_num]["pastTrips"],
                    "plannedFutureTrips": years_temp[year_num]["plannedFutureTrips"],
                    "futureTrips": years_temp[year_num]["futureTrips"],
                    "pastKm": years_temp[year_num]["pastKm"],
                    "plannedFutureKm": years_temp[year_num]["plannedFutureKm"],
                    "futureKm": years_temp[year_num]["futureKm"],
                }
            )
        else:
            years.append({
                "year": year_num,
                "pastTrips": 0,
                "plannedFutureTrips": 0,
                "futureTrips": 0,
                "pastKm": 0,
                "plannedFutureKm": 0,
                "futureKm": 0
            })
    
    # Add future if exists
    if future:
        years.append(
            {
                "year": lang["future"],
                "pastTrips": 0,
                "plannedFutureTrips": 0,
                "futureTrips": future["futureTrips"],
                "pastKm": 0,
                "plannedFutureKm": 0,
                "futureKm": future["futureKm"],
            }
        )
    
    return years


def get_stats_general(pg, query_func, user_id, stat_name, trip_type, year=None):
    """
    Generic stats fetcher for operators, material, routes, stations
    Now returns both Trips and Km data in unified format
    """
    result = pg.execute(
        query_func(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    stats = []
    for row in result:
        row_dict = dict(row._mapping)
        if row_dict.get(stat_name):
            stats.append(row_dict)
    return stats


def get_stats_routes(pg, user_id, trip_type, year=None):
    """
    Process route statistics with both trips and km
    """
    result = pg.execute(
        stats_sql.stats_routes(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    routes = []
    for row in result:
        row_dict = dict(row._mapping)
        routes.append({
            "route": row_dict["route"],
            "past": row_dict.get("pastTrips", 0),
            "plannedFuture": row_dict.get("plannedFutureTrips", 0),
            "future": row_dict.get("futureTrips", 0),
            "pastKm": row_dict.get("pastKm", 0),
            "plannedFutureKm": row_dict.get("plannedFutureKm", 0),
            "futureKm": row_dict.get("futureKm", 0),
            "count": row_dict.get("count", 0)
        })
    
    return routes


def get_stats_stations(pg, user_id, trip_type, year=None):
    """
    Process station statistics with both trips and km
    """
    result = pg.execute(
        stats_sql.stats_stations(),
        {"user_id": user_id, "tripType": trip_type, "year": year}
    ).fetchall()
    
    stations = []
    for row in result:
        row_dict = dict(row._mapping)
        stations.append({
            "station": row_dict["station"],
            "past": row_dict.get("pastTrips", 0),
            "plannedFuture": row_dict.get("plannedFutureTrips", 0),
            "pastKm": row_dict.get("pastKm", 0),
            "plannedFutureKm": row_dict.get("plannedFutureKm", 0),
            "count": row_dict.get("count", 0)
        })
    
    return stations


def fetch_stats(username, trip_type, year=None):
    """
    Fetch all statistics (both trips and km) in a single call
    If username is None, fetch stats for all users (admin mode)
    """
    stats = {}
    
    # Handle admin case - use None as user_id to get all users
    user_id = None if username is None else get_user_id(username)
    
    with pg_session() as pg:
        # Check if trip type is available for user (or any user if admin)
        available_types = pg.execute(
            stats_sql.type_available(),
            {"user_id": user_id}
        ).fetchall()
        
        type_exists = any(row[0] == trip_type for row in available_types)
        
        if not type_exists:
            return stats
            
        user_lang = session.get("userinfo", {}).get("lang", "en")
        lang_dict = lang.get(user_lang, {})
        
        # Fetch all stats with combined queries
        stats["operators"] = get_stats_general(
            pg=pg,
            query_func=stats_sql.stats_operator,
            user_id=user_id,
            stat_name="operator",
            trip_type=trip_type,
            year=year,
        )
        
        stats["material"] = get_stats_general(
            pg=pg,
            query_func=stats_sql.stats_material,
            user_id=user_id,
            stat_name="material",
            trip_type=trip_type,
            year=year,
        )
        
        stats["countries"] = get_stats_countries(
            pg=pg,
            user_id=user_id,
            trip_type=trip_type,
            year=year,
        )
        
        stats["years"] = get_stats_years(
            pg=pg,
            user_id=user_id,
            lang=lang_dict,
            trip_type=trip_type,
            year=year,
        )
        
        # Updated to use new functions
        stats["routes"] = get_stats_routes(
            pg=pg,
            user_id=user_id,
            trip_type=trip_type,
            year=year,
        )
        
        stats["stations"] = get_stats_stations(
            pg=pg,
            user_id=user_id,
            trip_type=trip_type,
            year=year,
        )

    return stats


def get_distinct_stat_years(username, trip_type):
    """Get list of years with statistics available"""
    user_id = None if username is None else get_user_id(username)
    
    with pg_session() as pg:
        result = pg.execute(
            stats_sql.distinct_stat_years(),
            {"user_id": user_id, "tripType": trip_type}
        ).fetchall()
        return [row[0] for row in result]