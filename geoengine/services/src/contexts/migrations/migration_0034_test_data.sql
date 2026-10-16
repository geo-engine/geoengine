INSERT INTO workflows (id, workflow) VALUES
(
    '00000000-0000-0000-0000-000000000001',
    '{
        "type": "Plot",
        "operator": {
            "type": "Statistics",
            "params": {"columnNames": ["alias"], "percentiles": []},
            "sources": {
                "source": [{"type": "GdalSource", "params": {"data": "ndvi"}}]
            }
        }
    }'
),
(
    '00000000-0000-0000-0000-000000000002',
    '{
        "type": "Plot",
        "operator": {
            "type": "BoxPlot",
            "params": {"columnNames": []},
            "sources": {
                "source": [{"type": "GdalSource", "params": {"data": "ndvi"}}]
            }
        }
    }'
),
(
    '00000000-0000-0000-0000-000000000003',
    '{
        "type": "Plot",
        "operator": {
            "type": "Statistics",
            "params": {"columnNames": [], "percentiles": []},
            "sources": {
                "source": [
                    {"type": "GdalSource", "params": {"data": "ndvi"}},
                    {"type": "GdalSource", "params": {"data": "ndvi"}}
                ]
            }
        }
    }'
),
(
    '00000000-0000-0000-0000-000000000004',
    '{
        "type": "Plot",
        "operator": {
            "type": "Statistics",
            "params": {"columnNames": ["x"], "percentiles": []},
            "sources": {
                "source": {"type": "OgrSource", "params": {"data": "points"}}
            }
        }
    }'
),
(
    '00000000-0000-0000-0000-000000000005',
    '{
        "type": "Plot",
        "operator": {
            "type": "Histogram",
            "params": {"attributeName": "band"},
            "sources": {
                "source": {"type": "GdalSource", "params": {"data": "ndvi"}}
            }
        }
    }'
);
