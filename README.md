# RUCO Oil Tracker

Real-time tracking of used cooking oil quality per fryer, built with Apache Kafka and
Spark Structured Streaming. Mini project for *Data Analytics for Smart & Sustainable Systems* (DASSS).

> Full setup guide, architecture diagram and results will be added in the final step.

## Dataset
Mendeley "Frying oil data", DOI [10.17632/4k7b395yxx.1](https://doi.org/10.17632/4k7b395yxx.1):
acid value, carbonyl value and Total Polar Compounds (TPC) of oil used to fry potato chips
at 160 / 175 / 190 / 205 / 220 °C, hours 0–30.

## Future work

### Arrhenius-style degradation rate
The model `tpc ~ hour + temp + hour×temp` assumes the degradation rate grows **linearly**
with temperature: `rate(T) = b_hour + b_int × T`. The lab data suggests it grows faster than
linearly (closer to exponential, as in the Arrhenius equation for chemical reaction rates):

| Fryer | Model rate (%/h) | Lab slope (%/h) |
|---|---|---|
| 160 °C | 0.204 | 0.298 |
| 175 °C | 0.457 | 0.347 |
| 190 °C | 0.710 | 0.724 |
| 205 °C | 0.962 | 0.937 |
| 220 °C | 1.215 | 1.317 |

A future version could model `log(rate) = a + b × T` (rate = A·e^(bT)), which would fix the
under-estimate at 160 °C and over-estimate at 175 °C. We did not implement it because it
would rest on only 5 temperature points; instead, the live stream corrects the rate with each
fryer's own measured slope (see `streaming/stream_processor.py`).
