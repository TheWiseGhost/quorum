package io.quorum.check;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Renders one key's history as a standalone HTML timeline (one lane per client). */
public final class HistoryVisualizer {
    private HistoryVisualizer() {}

    public static String render(String title, LinearizabilityChecker.KeyResult r) {
        List<Op> ops = r.ops();
        long t0 = ops.stream().mapToLong(Op::invoke).min().orElse(0);
        long t1 = ops.stream().mapToLong(o -> o.status() == Op.Status.INFO ? o.invoke() : o.complete()).max().orElse(1);
        t1 = Math.max(t1 + (t1 - t0) / 10 + 1, t0 + 1);
        TreeSet<Integer> clients = new TreeSet<>();
        ops.forEach(o -> clients.add(o.client()));
        int laneH = 34, width = 1200, left = 90;
        int height = clients.size() * laneH + 70;
        double scale = (width - left - 20) / (double) (t1 - t0);
        Set<Integer> good = r.bestPartial();

        StringBuilder svg = new StringBuilder();
        svg.append(String.format("<svg viewBox=\"0 0 %d %d\" width=\"100%%\" role=\"img\">", width, height));
        int lane = 0;
        for (int c : clients) {
            int y = 40 + lane * laneH;
            svg.append(String.format("<text x=\"8\" y=\"%d\" class=\"lane\">client %d</text>", y + 18, c));
            svg.append(String.format("<line x1=\"%d\" x2=\"%d\" y1=\"%d\" y2=\"%d\" class=\"grid\"/>", left, width - 10, y + laneH - 2, y + laneH - 2));
            for (Op o : ops) {
                if (o.client() != c) continue;
                double x = left + (o.invoke() - t0) * scale;
                long end = o.status() == Op.Status.INFO ? t1 : o.complete();
                double w = Math.max(3, (end - o.invoke()) * scale);
                String cls = o.status() == Op.Status.INFO ? "info" : good.contains(o.id()) ? "lin" : "bad";
                svg.append(String.format("<g><title>%s  [%d → %s]</title><rect x=\"%.1f\" y=\"%d\" width=\"%.1f\" height=\"22\" rx=\"4\" class=\"%s\"/>"
                                + "<text x=\"%.1f\" y=\"%d\" class=\"lbl\">%s</text></g>",
                        esc(o.describe()), o.invoke(), o.status() == Op.Status.INFO ? "?" : Long.toString(o.complete()),
                        x, y + 4, w, cls, x + 4, y + 19, esc(o.describe())));
            }
            lane++;
        }
        svg.append("</svg>");
        return """
                <!doctype html><html><head><meta charset="utf-8"><title>Linearizability failure</title>
                <style>
                :root{--bg:#fbfaf7;--fg:#1d1d1b;--muted:#6b6b66;--lin:#cfe8d5;--lin-b:#2f7a45;--bad:#f6d3cf;--bad-b:#b3261e;--info:#e7e3f3;--info-b:#6750a4;--grid:#e6e3dc}
                @media (prefers-color-scheme: dark){:root{--bg:#161614;--fg:#ecebe7;--muted:#a3a29c;--lin:#1f3b28;--lin-b:#7fd39a;--bad:#3e1f1c;--bad-b:#ff8a80;--info:#2a2540;--info-b:#cbb8ff;--grid:#2c2b28}}
                body{background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,sans-serif;margin:24px}
                h1{font-size:18px;margin:0 0 4px} p{color:var(--muted);margin:0 0 16px}
                .lane{fill:var(--muted);font-size:12px}.grid{stroke:var(--grid)}
                .lbl{fill:var(--fg);font-size:11px;pointer-events:none}
                .lin{fill:var(--lin);stroke:var(--lin-b)}.bad{fill:var(--bad);stroke:var(--bad-b)}.info{fill:var(--info);stroke:var(--info-b);stroke-dasharray:4 3}
                .legend span{display:inline-block;margin-right:16px}.sw{display:inline-block;width:12px;height:12px;border-radius:3px;vertical-align:-1px;margin-right:6px}
                </style></head><body>
                """ + "<h1>" + esc(title) + "</h1><p>Key <b>" + esc(r.key()) + "</b>: " + ops.size()
                + " operations, no valid linearization. Green ops form the longest partial linearization; red ops could not be placed after it.</p>"
                + "<p class=\"legend\"><span><i class=\"sw\" style=\"background:var(--lin)\"></i>linearized</span>"
                + "<span><i class=\"sw\" style=\"background:var(--bad)\"></i>not linearizable</span>"
                + "<span><i class=\"sw\" style=\"background:var(--info)\"></i>timed out (outcome unknown)</span></p>"
                + svg + "</body></html>";
    }

    private static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
