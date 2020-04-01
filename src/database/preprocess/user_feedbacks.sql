INSERT INTO user_feedbacks (epk_id, content_id, feedback, feedback_ts, update_ts)
SELECT epk_id
    , content_id
    , CASE
        WHEN dislike_flg = 1 THEN -1
        ELSE feedback
      END feedback
    , feedback_ts
    , update_ts
FROM (
    SELECT epk_id
         , content_id
         , MAX(CASE
                   WHEN event_type = 'Show' THEN 0
                   WHEN event_type = 'Click' THEN 1
                   WHEN event_type = 'Complete' THEN 3
                   WHEN event_type = 'Like' THEN 5
        END)                                                  feedback
         , MAX(CASE WHEN event_type = 'Dislike' THEN 1 ELSE 0 END) dislike_flg
         , MAX(event_ts)                                      feedback_ts
         , NOW() as                                           update_ts
    FROM clickstream_sa
    WHERE 1=1
      AND (event_type != 'Show'
       OR  event_type = 'Show' AND  event_ts >= current_date - 3
          )
    GROUP BY epk_id, content_id
)q
;
