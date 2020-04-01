INSERT INTO item_popularity (content_id, show_cnt, click_cnt, complete_cnt, like_cnt, dislike_cnt, click_rate, click_rate_lb, click_rate_ub, actual_from, actual_to, update_ts)
SELECT q2.content_id
     , q2.show_cnt
     , q2.click_cnt
     , q2.complete_cnt
     , q2.like_cnt
     , q2.dislike_cnt
     , click_rate
     , click_rate - 1.96*sqrt(click_rate*(1-click_rate) / show_cnt) click_rate_lb
     , click_rate + 1.96*sqrt(click_rate*(1-click_rate) / show_cnt) click_rate_ub
     , c.actual_from
     , c.actual_to
     , q2.update_ts
FROM (
         SELECT content_id
              , show_cnt
              , click_cnt
              , complete_cnt
              , like_cnt
              , dislike_cnt
              , cast(click_cnt as float) / cast(show_cnt as float) as click_rate
              , NOW()                                              as update_ts
         FROM (
                  SELECT content_id
                       , SUM(CASE WHEN event_type = 'Show' THEN 1 ELSE 0 END)     show_cnt
                       , SUM(CASE WHEN event_type = 'Click' THEN 1 ELSE 0 END)    click_cnt
                       , SUM(CASE WHEN event_type = 'Complete' THEN 1 ELSE 0 END) complete_cnt
                       , SUM(CASE WHEN event_type = 'Like' THEN 1 ELSE 0 END)     like_cnt
                       , SUM(CASE WHEN event_type = 'Dislike' THEN 1 ELSE 0 END)  dislike_cnt
                  FROM clickstream_sa
                  WHERE 1 = 1
                  GROUP BY content_id
              ) q
     )q2 INNER JOIN ref_content c
            ON (c.content_id = q2.content_id)
;
