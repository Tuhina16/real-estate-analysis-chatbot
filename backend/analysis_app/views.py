from rest_framework.views import APIView
from rest_framework.response import Response
from .utils import (
    load_sheet_df,
    filter_area,
    extract_locations_from_query,
    extract_year_span,
    pick_metric_column
)
import pandas as pd


class AnalyzeView(APIView):

    def post(self, request):
        query = request.data.get("query", "")
        if not query:
            return Response({"error": "Query is required"}, status=400)

        df = load_sheet_df()

        locations = extract_locations_from_query(query, df)

        if len(locations) == 0:
            return Response({
                "summary": "No known location found in your query.",
                "chart": {},
                "table": []
            })

        year_span = extract_year_span(query)
        current_year = df["year"].max()

        if len(locations) == 1:
            area = locations[0]
            df_area = filter_area(df, area)

            if df_area.empty:
                return Response({
                    "summary": f"No data found for {area}.",
                    "chart": {},
                    "table": []
                })

            if year_span:
                start_year = current_year - year_span + 1
                df_area = df_area[df_area["year"] >= start_year]

            metric_col = pick_metric_column(df_area, query)
            year_col = "year"

            chart = {}
            if metric_col:
                g = df_area.groupby(year_col)[metric_col].mean()
                chart = {
                    "years": list(g.index.astype(str)),
                    "values": [float(v) for v in g.values],
                    "metric": metric_col
                }

            summary = f"Analysis for {area}."
            if year_span:
                summary += f" Showing last {year_span} years."

            table = df_area.fillna("").to_dict(orient="records")

            return Response({
                "summary": summary,
                "chart": chart,
                "table": table
            })

        if len(locations) >= 2:
            loc1, loc2 = locations[0], locations[1]

            df1 = filter_area(df, loc1)
            df2 = filter_area(df, loc2)

            if df1.empty or df2.empty:
                return Response({
                    "summary": f"Not enough data to compare {loc1} and {loc2}.",
                    "chart": {},
                    "table": []
                })

            if year_span:
                start_year = current_year - year_span + 1
                df1 = df1[df1["year"] >= start_year]
                df2 = df2[df2["year"] >= start_year]

            year_col = "year"
            metric_col = pick_metric_column(df, query)

            common_years = sorted(
                set(df1[year_col]).intersection(set(df2[year_col]))
            )

            chart = {
                "comparison": f"{loc1} vs {loc2}",
                "loc1": loc1,
                "loc2": loc2,
                "years": [str(y) for y in common_years],
                "loc1_values": [],
                "loc2_values": [],
                "metric": metric_col
            }

            for y in common_years:
                v1 = df1[df1[year_col] == y][metric_col].mean()
                v2 = df2[df2[year_col] == y][metric_col].mean()
                chart["loc1_values"].append(float(v1))
                chart["loc2_values"].append(float(v2))

            summary = f"Comparison of {loc1} and {loc2}"
            if year_span:
                summary += f" over the last {year_span} years."

            return Response({
                "summary": summary,
                "chart": chart,
                "table": []
            })
