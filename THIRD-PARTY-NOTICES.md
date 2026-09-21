# Third-Party Notices

This product bundles the following third-party software. Each is reproduced below
with its copyright and permission notice, as its license requires.

Why this file exists: the upstream `telegraf` image ships no LICENSE file, so a
derived image that omits this one distributes Telegraf without its notice. See
the decision memo (`docs/DECISION-live-pipeline-simplification.md`, Proposal A)
for which distribution forms trigger the obligation.

---

## Telegraf

https://github.com/influxdata/telegraf — bundled in the producer image
(`services/producer_service/Dockerfile`), unmodified, as the execd runtime.

```
The MIT License (MIT)

Copyright (c) 2015-2025 InfluxData Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
