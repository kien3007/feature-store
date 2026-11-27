# Kế hoạch dự án: Feature Store với Feast – cập nhật định kỳ và giám sát sức khỏe snapshot

Nov 26, 2026 · @KienNguyen

## 1. Mục tiêu và phạm vi

Dự án dựng một feature store chạy hoàn toàn local bằng Feast, tự cập nhật theo lịch, và chấm điểm sức khỏe từng snapshot trước khi cho nó vào online store. Phần kiểm tra chất lượng mỗi lần cập nhật là phần tự dựng của dự án, đặt cạnh Feast chứ không sửa Feast.

**Bốn kết quả cần đạt**

1. Cập nhật định kỳ: mỗi chu kỳ (mặc định hằng ngày) tự tạo một snapshot feature mới, không cần can thiệp tay.
2. Mỗi snapshot có bộ metrics sức khỏe và trạng thái pass / warn / fail, lưu lại để tra cứu theo thời gian.
3. Snapshot fail không vào online store; online store vẫn giữ snapshot tốt gần nhất.
4. Có dashboard xem xu hướng, và một kịch bản demo chủ động tiêm lỗi để chứng minh cổng chất lượng hoạt động.

**Snapshot là gì.** Một snapshot là toàn bộ giá trị feature tính tại một mốc `snapshot_ts`, chỉ dùng dữ liệu có timestamp nhỏ hơn mốc đó. Chạy lại cùng mốc phải ra cùng kết quả.

**Ngoài phạm vi.** Streaming, serving độ trễ thấp, nhiều môi trường dev/staging/prod và Kubernetes. Những phần này thuộc các dự án mở rộng khác.

## 2. Kiến trúc tổng thể

Chuỗi xử lý chỉ có một cổng: Feast đọc dữ liệu từ Published, nên snapshot chưa qua kiểm tra không thể vào training hay serving.

&#91;embedded content: kiến trúc · 10 thành phần, 1 cổng kiểm tra\]

Đọc hàng trên từ trái sang phải: pass hoặc warn đi tiếp sang Published, fail rẽ xuống Quarantine, còn mọi lần chạy đều ghi metrics cho dashboard.

| Thành phần | Lựa chọn | Lý do |
| --- | --- | --- |
| Feature store | Feast | Registry, API offline/online chuẩn, đúng trọng tâm của đề bài |
| Transform và check | DuckDB | Chạy trong tiến trình, đọc Parquet theo cột, vừa máy 16 GB; dùng SQL cho cả hai việc |
| Định dạng dữ liệu | Parquet | Dạng cột, nén tốt; Feast đọc trực tiếp làm batch source |
| Kho metrics | PostgreSQL | Pipeline ghi và Grafana đọc cùng lúc mà không vướng khóa file; Grafana có sẵn nguồn dữ liệu PostgreSQL, không cần cài plugin |
| Điều phối | Airflow 3 | Lập lịch theo ngày, backfill theo logical date, rẽ nhánh cho cổng; chạy local với LocalExecutor và metadata DB trên PostgreSQL |
| Drift và báo cáo | Evidently | Có sẵn so sánh phân phối và báo cáo HTML; check còn lại tự viết bằng SQL |
| Online store | SQLite | Mặc định khi chạy local, không cần dựng Redis |
| Dashboard và cảnh báo | Grafana | Panel chuỗi thời gian, vạch ngưỡng và alert rule; chạy bằng Docker Compose cùng PostgreSQL |

## 3. Cấu trúc repo

Repo tách ba lớp rõ ràng: định nghĩa Feast (`feature_repo/`), logic tính và kiểm tra (`src/fsmon/`), và điều phối (`dags/`). DAG chỉ gọi lệnh `fsmon` nên rất mỏng, và Airflow chạy trong virtualenv riêng để không xung đột phụ thuộc với Feast và Evidently. Cấu hình và ngưỡng cảnh báo nằm trong YAML để đổi mà không sửa code.

```text
feature-store-monitor/
├─ pyproject.toml            # uv; ghim phiên bản feast, duckdb, evidently (môi trường của pipeline)
├─ Makefile                  # make up | airflow | snapshot | check | publish | demo-fault
├─ docker-compose.yml        # PostgreSQL (metrics + metadata Airflow) + Grafana + server tĩnh cho báo cáo Evidently
├─ airflow/                  # môi trường riêng cho Airflow: constraints, biến môi trường, hướng dẫn cài
├─ dags/                     # DAG Airflow mỏng: snapshot_pipeline.py, chỉ gọi lệnh fsmon
├─ config/
│  ├─ pipeline.yaml          # nguồn dữ liệu, chu kỳ, đường dẫn, DSN PostgreSQL, giới hạn bộ nhớ
│  └─ checks.yaml            # hợp đồng schema + ngưỡng warn/fail theo feature view
├─ data/                     # trong .gitignore
│  ├─ raw/                   # dữ liệu gốc
│  ├─ staging/               # snapshot_ts=YYYY-MM-DD/<view>.parquet (chưa duyệt)
│  ├─ published/             # <view>.parquet (đã qua cổng, là batch source của Feast)
│  ├─ quarantine/            # snapshot bị fail, giữ để điều tra
│  └─ reports/               # báo cáo Evidently HTML của snapshot warn hoặc fail
├─ sql/schema.sql            # bảng snapshot_runs và snapshot_metrics trong PostgreSQL
├─ feature_repo/             # repo Feast
│  ├─ feature_store.yaml
│  ├─ entities.py
│  ├─ data_sources.py
│  ├─ feature_views.py
│  └─ feature_services.py
├─ src/fsmon/
│  ├─ cli.py                 # lệnh fsmon: transform | audit | publish | verify | report
│  ├─ transform.py           # SQL DuckDB: raw -> staging cho một snapshot_ts
│  ├─ checks/                # schema, volume, freshness, null, range, drift
│  ├─ runner.py              # chạy check, ghi metrics vào PostgreSQL, trả về quyết định cổng
│  ├─ publish.py             # staging -> published, gọi Feast materialize
│  ├─ verify_online.py       # so khớp online với published sau khi publish
│  └─ alert.py               # ghi alerts.log, dự phòng khi Grafana tắt
├─ grafana/provisioning/     # datasource, dashboard JSON, alert rule, contact point
├─ scripts/inject_fault.py   # tiêm lỗi cho kịch bản demo
├─ tests/                    # pytest cho check, cổng, tính idempotent
└─ .github/workflows/ci.yml  # lint + test + feast plan
```

**Quy ước quan trọng:** dữ liệu chỉ đi một chiều raw → staging → published. Feast chỉ bao giờ đọc từ `published/`, nên snapshot chưa duyệt không thể lọt vào training hay serving.

## 4. Thiết kế Feast

Thiết kế dùng một entity chính, vài feature view chia theo cửa sổ thời gian, và một feature service cho mỗi mô hình. Tên thật sẽ chốt khi có dataset; bảng dưới là khuôn mẫu.

| Thành phần | Quy ước | Ví dụ (thay bằng dataset thật) |
| --- | --- | --- |
| Entity | Một khóa chính, cùng tên cột ở mọi view | `customer_id` (INT64) |
| Batch source | Parquet trong `data/published/`; `event_timestamp` = `snapshot_ts`; `created_timestamp` = thời điểm chạy | `published/customer_activity.parquet` |
| Feature view | Gom feature theo nguồn và cửa sổ; mỗi view một nhóm ý nghĩa | `customer_activity_7d`, `customer_rfm_90d`, `customer_profile` |
| Tên feature | `<đại lượng>_<cửa sổ>` | `purchase_cnt_30d`, `days_since_last_order` |
| TTL | Ít nhất 2 lần chu kỳ cập nhật, để một snapshot bị bỏ lỡ không làm feature thành null | Cập nhật hằng ngày → TTL 2–3 ngày |
| Tags | `owner`, `domain`, `contract_version` | Dùng để lọc trên dashboard |
| Feature service | Một service cho mỗi mô hình, liệt kê đúng feature mô hình dùng | `purchase_propensity_v1` |
| Online store | SQLite (mặc định khi chạy local) | File trong `data/` |
| Offline store | `file` là mặc định; thử `duckdb` nếu phiên bản Feast bạn ghim hỗ trợ | Chốt ở Giai đoạn 1 |
| Registry | File local; đưa vào CI để `feast plan` phát hiện thay đổi định nghĩa | `registry.db` |

**Bốn luật thiết kế**

1. Không dùng dữ liệu tương lai: feature của `snapshot_ts` chỉ dùng bản ghi có thời gian sự kiện nhỏ hơn mốc đó.
2. Một view một lịch cập nhật: feature đổi chậm (hồ sơ tĩnh) tách khỏi feature đổi nhanh.
3. Đổi ý nghĩa feature thì đổi tên hoặc tăng `contract_version`, không sửa im lặng.
4. Khai báo kiểu dữ liệu tường minh trong schema của view; check `schema_match` đọc hợp đồng từ `checks.yaml` và so với đúng schema này.

## 5. Pipeline cập nhật định kỳ

Mỗi lần chạy theo mẫu write–audit–publish: ghi ra vùng staging, kiểm tra, rồi mới publish. Một lần chạy DAG tương ứng một `snapshot_ts` (logical date của Airflow), nên chạy lại hay backfill quá khứ đều dùng cùng một đường.

1. **Kích hoạt.** DAG `snapshot_pipeline` chạy theo lịch (mặc định hằng ngày, đọc từ `pipeline.yaml`); logical date của lần chạy chính là `snapshot_ts`.
2. **Transform.** Task gọi `fsmon transform`: SQL DuckDB đọc dữ liệu raw có thời gian nhỏ hơn `snapshot_ts`, ghi `staging/snapshot_ts=.../<view>.parquet`.
3. **Audit.** Task `fsmon audit` chạy toàn bộ check trên staging và ghi metrics vào PostgreSQL, kể cả khi kết quả là fail.
4. **Cổng.** Task rẽ nhánh đọc quyết định của `runner.py`. Pass hoặc warn → đi tiếp. Fail → nhánh quarantine chuyển staging sang `quarantine/`, gửi cảnh báo, dừng; online store giữ snapshot cũ.
5. **Publish.** Xóa hàng có cùng `snapshot_ts` trong `published/<view>.parquet` rồi chèn snapshot mới (nhờ vậy chạy lại không sinh bản trùng), `feast apply` nếu định nghĩa đổi, rồi `feast materialize-incremental`.
6. **Xác minh online.** Lấy mẫu vài trăm entity, đọc `get_online_features` và so với `published/`; ghi tỷ lệ khớp vào metrics.
7. **Báo cáo.** Ghi dòng tóm tắt vào bảng `snapshot_runs`; Grafana đọc cùng bảng nên dashboard tự cập nhật, không cần bước đẩy dữ liệu.

**Tính chất cần giữ**

- Idempotent: chạy hai lần cùng `snapshot_ts` cho cùng kết quả và không nhân đôi dòng.
- Backfill được: chạy lại dải `snapshot_ts` trong quá khứ bằng backfill của Airflow 3 (giao diện hoặc CLI; kiểm tra cú pháp ở bản bạn ghim) để tạo lịch sử metrics. Nếu dataset có chiều thời gian, backfill 30 snapshot cho dashboard có xu hướng ngay mà không phải chờ 30 ngày.
- Thứ tự và tải: `max_active_runs=1` để snapshot publish đúng thứ tự thời gian và backfill không chạy song song làm tràn RAM.
- Task chỉ gọi CLI: mỗi task của DAG là một lệnh `fsmon ...` qua BashOperator, nên logic test được ngoài Airflow và Airflow dùng môi trường riêng.
- Bộ nhớ bị chặn: `SET memory_limit` cho DuckDB (gợi ý 6–8 GB trên máy 16 GB), đọc Parquet theo cột, không nạp cả bảng vào pandas.
- Thất bại an toàn: nếu bước 5 hoặc 6 lỗi, đánh dấu run là `failed_after_gate` và có thủ tục khôi phục: dựng lại online store từ snapshot tốt gần nhất trong thư mục published.

## 6. Metrics sức khỏe snapshot

Mỗi snapshot được đo bằng khoảng 14 metrics chia thành bảy nhóm: schema, khối lượng, độ tươi, giá trị, drift, đồng nhất online/offline và vận hành. Ngưỡng trong bảng là mặc định khởi đầu, đổi được trong `checks.yaml` sau khi xem phân phối thật của dataset.

| Nhóm | Metric | Đo trên | Warn | Fail |
| --- | --- | --- | --- | --- |
| Schema | `schema_match` | Tên cột và kiểu so với hợp đồng | Có cột lạ | Thiếu cột hoặc đổi kiểu |
| Schema | `key_duplicate_rate` | Tỷ lệ trùng cặp (entity, `snapshot_ts`) | — | > 0 |
| Khối lượng | `row_count_change` | % đổi so với trung vị 7 snapshot đã publish gần nhất | > 20% | > 50% |
| Khối lượng | `entity_coverage` | Số entity còn lại so với snapshot trước | < 98% | < 90% |
| Độ tươi | `freshness_lag` | `snapshot_ts` trừ thời gian sự kiện mới nhất của nguồn | > 1 chu kỳ | > 2 chu kỳ |
| Giá trị | `null_rate` | Từng feature; đổi so với baseline (điểm phần trăm) | +5 | +20 |
| Giá trị | `inf_nan_count` | Số giá trị inf hoặc NaN ngoài null | — | > 0 |
| Giá trị | `out_of_range_rate` | % ngoài miền khai báo (ví dụ số đếm ≥ 0) | > 0 | > 1% |
| Giá trị | `distinct_count` | Feature rời rạc; phát hiện feature thành hằng số | = 1 | — |
| Drift | `psi` | PSI mỗi feature so với baseline | > 0,1 | > 0,25 |
| Drift | `mean_shift_z` | (trung bình mới − trung bình baseline) / độ lệch chuẩn baseline | > 3 | > 5 |
| Online/offline | `online_match_rate` | Tỷ lệ entity mẫu mà online khớp `published/` | < 99,9% | < 99% |
| Vận hành | `run_duration_s` | Tổng thời gian chạy | > 2× trung vị | — |
| Vận hành | `online_get_p95_ms` | Độ trễ p95 khi xác minh online | Chỉ ghi nhận | — |

Ngưỡng PSI 0,1 và 0,25 là quy ước phổ biến trong ngành, không phải tiêu chuẩn cứng; hiệu chỉnh theo dataset. PSI chia feature thành các bin dựa trên baseline rồi cộng độ lệch tỷ lệ theo công thức:

```latex
\mathrm{PSI} = \sum_{i=1}^{k} (a_i - e_i)\,\ln\frac{a_i}{e_i}
```

Trong đó `e_i` là tỷ lệ dòng ở bin `i` của baseline, `a_i` là tỷ lệ ở snapshot mới, `k` là số bin (mặc định 10, cộng một bin riêng cho null). Cộng một hằng số nhỏ vào tỷ lệ rỗng để tránh chia cho 0.

**Hai bảng lưu kết quả trong PostgreSQL**

| Bảng | Cột | Vai trò |
| --- | --- | --- |
| `snapshot_runs` | `run_id`, `snapshot_ts`, `status` (published / quarantined / failed\_after\_gate), `started_at`, `finished_at`, `git_sha`, `feast_version`, `row_count` | Một dòng cho mỗi lần chạy; Grafana lấy `finished_at` để đo tuổi snapshot đang phục vụ |
| `snapshot_metrics` | `run_id`, `snapshot_ts`, `feature_view`, `feature` (null nếu ở mức view), `check_name`, `metric_name`, `value`, `warn_threshold`, `fail_threshold`, `status`, `baseline_ref` | Dạng dài, mỗi metric một dòng, thêm metric mới không đổi schema; `snapshot_ts` kiểu `timestamptz` làm cột thời gian cho panel Grafana, có chỉ mục theo (`feature_view`, `metric_name`, `snapshot_ts`) |

## 7. Ngưỡng cảnh báo và cổng chặn

Snapshot chỉ bị chặn khi một check loại blocker fail; lệch phân phối chỉ cảnh báo, vì drift có thể phản ánh thay đổi thật của hành vi chứ không phải lỗi dữ liệu. Bạn có thể đổi loại của từng check trong `checks.yaml`.

| Loại check | Gồm | Khi fail |
| --- | --- | --- |
| Blocker | `schema_match`, `key_duplicate_rate`, `row_count_change`, `entity_coverage`, `freshness_lag`, `inf_nan_count`, `out_of_range_rate`, `null_rate` | Quarantine, không publish |
| Advisory | `psi`, `mean_shift_z`, `distinct_count`, `run_duration_s` | Publish kèm cảnh báo |
| Sau publish | `online_match_rate` | Đánh dấu `failed_after_gate`, khôi phục online store từ snapshot tốt trước |

| Trạng thái snapshot | Điều kiện | Hành động |
| --- | --- | --- |
| pass | Mọi check pass | Publish |
| warn | Không có blocker fail, nhưng có warn hoặc advisory fail | Publish, cảnh báo, gắn nhãn trên dashboard |
| fail | Ít nhất một blocker fail | Quarantine, cảnh báo, online store giữ snapshot cũ |

**Quy tắc vận hành**

- Baseline là trung vị của tối đa 7 snapshot đã publish gần nhất. Snapshot bị quarantine không bao giờ vào baseline, để dữ liệu hỏng không kéo ngưỡng lệch theo.
- Khởi động nguội: 7 snapshot đầu chưa đủ baseline nên chỉ áp ngưỡng tuyệt đối (schema, khóa, miền giá trị, inf/NaN, độ tươi). Check tương đối vẫn ghi metric nhưng gắn nhãn `baseline_incomplete`.
- Ghi đè thủ công: `make approve SNAPSHOT=<ts> REASON="..."` publish một snapshot đang quarantine, và lưu lý do vào `snapshot_runs`.
- Cảnh báo: Grafana alert rule đọc `snapshot_runs` và `snapshot_metrics` rồi gửi qua contact point (email, Slack, Discord hoặc Telegram). `runner.py` vẫn ghi `alerts.log` để có dấu vết khi Grafana tắt.
- Cổng không phụ thuộc Grafana: quyết định pass/warn/fail luôn do `runner.py` đưa ra; Grafana chỉ hiển thị và báo, nên tắt Grafana không làm hỏng pipeline.
- Cảnh báo pipeline ngừng chạy: một rule riêng báo khi snapshot publish gần nhất cũ hơn 1,5 chu kỳ, vì khi pipeline không chạy thì không có check nào fail để báo.
- Hiệu chỉnh ngưỡng: sau khi backfill, nếu hơn 20% snapshot của dữ liệu bình thường bị warn thì ngưỡng đang quá chặt, nới ra trước khi cho chạy định kỳ. Con số 20% là mốc khởi đầu, không phải chuẩn.

## 8. Dashboard và báo cáo

Grafana chạy bằng Docker Compose cùng PostgreSQL và tự nạp datasource, dashboard, alert rule từ thư mục `grafana/provisioning`, nên dựng lại từ máy sạch chỉ cần `make up`. Dashboard có ba trang, mỗi trang trả lời một câu hỏi; giao diện web của Feast (`feast ui`) vẫn dùng để xem registry.

| Dashboard | Câu hỏi trả lời | Panel chính |
| --- | --- | --- |
| Tổng quan | Snapshot gần đây có khỏe không? | State timeline pass/warn/fail theo feature view cho 30 snapshot gần nhất; stat số run bị quarantine và tuổi snapshot đang phục vụ online |
| Chi tiết feature | Feature nào đang xấu đi? | Time series `null_rate`, `psi` và trung bình kèm đường ngưỡng warn/fail; bảng xếp hạng PSI cao nhất; biến chọn feature view và feature |
| Nhật ký chạy | Chuyện gì xảy ra ở lần chạy này? | Bảng `snapshot_runs`, lý do fail, liên kết tới báo cáo Evidently HTML, lịch sử duyệt tay |

Báo cáo Evidently HTML chỉ sinh cho snapshot warn hoặc fail, lưu trong `data/reports/` và phục vụ bằng một server tĩnh nhỏ trong Compose để dashboard đặt được đường liên kết. Dashboard, datasource và alert rule đều nằm trong repo dưới dạng file, nên mọi thay đổi được review và hoàn tác như code.

## 9. Lịch trình theo giai đoạn

Dự án cần khoảng 56–68 giờ, tương đương khoảng 1,5 tuần toàn thời gian hoặc 5–7 tuần nếu dành 10–12 giờ mỗi tuần. Số giờ là ước tính của mình, chưa dựa trên dataset cụ thể; giai đoạn 3 chiếm nhiều giờ nhất vì gồm dựng Airflow, Grafana, PostgreSQL và alert rule dưới dạng file.

&#91;embedded content: lộ trình · 5 giai đoạn, 5 cổng, 56–68 giờ\]

Mỗi giai đoạn khép bằng một cổng; chưa qua cổng thì chưa sang giai đoạn sau.

| Giai đoạn | Việc chính | Sản phẩm để kiểm |
| --- | --- | --- |
| 0 | Cài WSL2, Docker, uv, ghim phiên bản Feast; nạp dataset vào `data/raw`; chọn entity và danh sách feature đầu tiên | `feast --version` chạy được; `docker compose up` lên được PostgreSQL và Grafana; một notebook mô tả dữ liệu |
| 1 | Viết `transform.py`, `cli.py` và định nghĩa Feast; chạy tay `feast apply` và `feast materialize-incremental` cho một `snapshot_ts` | Đọc được feature từ online store và dựng được training set bằng `get_historical_features` |
| 2 | Viết các check, `runner.py`, quarantine và hai bảng metrics trong PostgreSQL; test cho từng blocker | `pytest` xanh; một snapshot lỗi bị chặn đúng |
| 3 | Cài Airflow trong virtualenv riêng (LocalExecutor, metadata DB trên PostgreSQL), viết DAG gọi CLI `fsmon`, backfill 30 snapshot, dashboard và alert rule Grafana dưới dạng file, hiệu chỉnh ngưỡng | Dashboard có xu hướng 30 snapshot; DAG tự chạy 3 chu kỳ; một alert thử gửi được |
| 4 | `inject_fault.py`, CI, README, quay demo | Demo 5 phút chạy không sửa tay |

## 10. Tiêu chí hoàn thành, demo và rủi ro

Dự án xong khi chạy hết kịch bản demo mà không sửa tay và mọi mục dưới đây được tick.

- [ ] `make snapshot` chạy trọn chuỗi raw → staging → audit → publish → materialize cho một `snapshot_ts` mà không lỗi
- [ ] Chạy lại cùng `snapshot_ts` hai lần cho cùng số dòng và không sinh bản trùng (có test pytest)
- [ ] Backfill ít nhất 30 snapshot và dashboard Grafana hiển thị xu hướng
- [ ] Schedule tự chạy ít nhất 3 chu kỳ liên tiếp không cần can thiệp
- [ ] Mỗi check loại blocker có test chứng minh nó chặn đúng snapshot lỗi
- [ ] Sau mỗi lần fail, online store vẫn giữ snapshot tốt gần nhất
- [ ] `make up` dựng lại PostgreSQL và Grafana từ máy sạch; dashboard và alert rule tự nạp, một alert thử gửi được thông báo
- [ ] CI xanh: lint, test và `feast plan`
- [ ] README có sơ đồ kiến trúc, hướng dẫn chạy từ máy sạch và ảnh chụp dashboard

**Kịch bản demo (khoảng 5 phút)**

1. Chạy một snapshot bình thường; dashboard Grafana hiện pass và online store cập nhật.
2. `inject_fault.py --fault null_spike` đẩy tỷ lệ null của một feature lên thêm 30 điểm phần trăm: `null_rate` fail, snapshot vào quarantine, online store không đổi, và alert rule gửi cảnh báo.
3. `--fault schema_drop` bỏ một cột: `schema_match` fail.
4. `--fault duplicate_keys` nhân đôi một phần entity: `key_duplicate_rate` fail.
5. `--fault shift` nhân một feature số với hằng số: `psi` vượt ngưỡng, snapshot vẫn publish nhưng có cảnh báo, cho thấy sự khác nhau giữa blocker và advisory.
6. Duyệt tay một snapshot đang quarantine bằng `make approve` và xem dòng lý do trong nhật ký chạy.

**Rủi ro**

| Rủi ro | Hậu quả | Cách xử lý |
| --- | --- | --- |
| Feast trên Windows native có thể lỗi đường dẫn hoặc phụ thuộc | Mất thời gian ở Giai đoạn 0 | Dùng WSL2 (Ubuntu) làm môi trường chính, đặt repo trong hệ file của WSL chứ không đặt trong `/mnt/c`; cài thử ngay ở Giai đoạn 0 |
| Bộ nhớ 16 GB | Tiến trình bị kill khi bảng lớn | `memory_limit` DuckDB 6–8 GB; giới hạn WSL khoảng 10–12 GB trong `.wslconfig`; phát triển trên tập con dữ liệu; không dùng Spark hay Kafka |
| Docker, PostgreSQL, Grafana và Airflow cùng chạy trong WSL2 | Thêm RAM và thêm dịch vụ phải vận hành | Dùng Docker Engine trong WSL2 (hoặc Docker Desktop nếu máy đã cài); nằm trong giới hạn `.wslconfig`; đo mức dùng RAM thật ở Giai đoạn 0 và 3 thay vì tin ước tính |
| Airflow kéo nhiều phụ thuộc, dễ xung đột với Feast và Evidently | Cài đặt lỗi hoặc phải hạ phiên bản thư viện | Cài Airflow trong virtualenv riêng theo constraints file chính thức; DAG chỉ gọi CLI `fsmon`; dùng LocalExecutor, đặt `max_active_runs=1` và giới hạn `parallelism` |
| Dashboard chỉnh tay trên giao diện rồi quên xuất ra file | Mất cấu hình khi dựng lại | Giữ dashboard và alert rule trong `grafana/provisioning`; xuất JSON sau mỗi lần sửa và commit |
| Feast hoặc Airflow đổi hành vi giữa các phiên bản | Lệnh hoặc cấu hình không còn đúng | Ghim phiên bản trong `pyproject.toml` và file constraints của Airflow, ghi `feast_version` vào `snapshot_runs`, đọc tài liệu đúng phiên bản đã ghim |
| Ngưỡng sai gây báo động giả | Mất tin vào cổng chặn | Hiệu chỉnh sau backfill (mục 7); mọi ngưỡng nằm trong YAML |
| Dataset không có chiều thời gian | Không backfill được, khó chứng minh drift | Chia dữ liệu thành chuỗi lát cắt theo thứ tự giả lập và dùng kịch bản tiêm lỗi làm nguồn drift |
| Phạm vi phình to | Dự án không xong | Giữ danh sách ngoài phạm vi ở mục 1; chỉ thêm streaming sau khi đạt hết tiêu chí |

## 11. Thông tin cần từ bạn để chốt kế hoạch

Không có dataset thì kế hoạch vẫn chạy được ở mức khuôn mẫu; chỉ có dataset là thiếu thông tin thật. Các mặc định dưới đây sẽ được dùng nếu bạn không đổi.

| Câu hỏi | Mặc định nếu bạn không đổi |
| --- | --- |
| Dataset: nguồn, dung lượng, khóa entity, có cột thời gian không? | Chưa có; cần bạn gửi mô tả hoặc vài dòng mẫu |
| Chu kỳ snapshot | Hằng ngày |
| Có backfill lịch sử không? | Có, 30 snapshot nếu dữ liệu có chiều thời gian |
| Môi trường phát triển | WSL2 Ubuntu trên Windows |
| Docker | Docker Engine trong WSL2; dùng Docker Desktop nếu máy đã cài |
| Kênh nhận cảnh báo | Giao diện Grafana và `alerts.log`; thêm email hoặc Telegram ở Giai đoạn 3 nếu bạn muốn |
| Trình điều phối | Airflow 3 trong virtualenv riêng, LocalExecutor, metadata DB là một database riêng trong PostgreSQL của Compose |
| Thời gian dành cho dự án mỗi tuần | Ước tính tổng 56–68 giờ; cho mình biết giờ mỗi tuần để chia lịch |
| Mô hình hạ nguồn | Một mô hình LightGBM nhỏ đọc từ feature service để chứng minh train/serve nhất quán; bỏ nếu không cần |

Khi có dataset, mình sẽ điền tên entity, feature view và ngưỡng cụ thể vào mục 4 và mục 7.
