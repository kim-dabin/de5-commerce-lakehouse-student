# Olist classroom sample

이 디렉터리는 Olist 원본 데이터에서 2,000개 주문을 골라 만든 수업용 샘플입니다.

```text
ux_events.jsonl
  - 주문/상품/리뷰 관계를 기반으로 생성한 append-only UXLog
  - search_result_click, product_view, review_impression, review_expand, add_to_cart, purchase, remove_from_cart
  - 리뷰 감성에 따른 browse-only 세션을 추가 생성해 리뷰 노출 이후 이탈/전환 분석을 관찰할 수 있게 함

review_events.jsonl
  - Olist review를 기반으로 만든 review 상태 변경 이벤트
  - review_created, sentiment_scored, review_answered

order_status_events.jsonl
  - Olist order timestamp를 기반으로 만든 order 상태 변경 이벤트
  - order_created, order_approved, order_shipped, order_delivered, order_canceled

product_xref.csv
  - Olist 원본 product_id와 수업용 numeric product_id/catalog_id/category_id 매핑
```

샘플 생성 명령입니다.

```bash
./data/generate_olist_lakehouse_events.py \
  --input-dir data/raw/olist \
  --output-dir data/sample/olist \
  --max-orders 2000 \
  --order-offset 40000
```

검증된 샘플 row count입니다.

```text
ux_events.jsonl            16,693
review_events.jsonl         5,943
order_status_events.jsonl   7,886
```
