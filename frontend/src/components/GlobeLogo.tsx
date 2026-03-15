"use client";

import { useEffect, useRef } from "react";

export function GlobeLogo({ size = 40 }: { size?: number }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let animId: number;
    let angle = 0;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = size * dpr;
    canvas.height = size * dpr;
    ctx.scale(dpr, dpr);

    const NUM_POINTS = 60;
    const points: { x: number; y: number; z: number }[] = [];
    const goldenAngle = Math.PI * (3 - Math.sqrt(5));

    for (let i = 0; i < NUM_POINTS; i++) {
      const y = 1 - (i / (NUM_POINTS - 1)) * 2;
      const radius = Math.sqrt(1 - y * y);
      const theta = goldenAngle * i;
      points.push({
        x: Math.cos(theta) * radius,
        y,
        z: Math.sin(theta) * radius,
      });
    }

    const connections: [number, number][] = [];
    for (let i = 0; i < points.length; i++) {
      for (let j = i + 1; j < points.length; j++) {
        const dx = points[i].x - points[j].x;
        const dy = points[i].y - points[j].y;
        const dz = points[i].z - points[j].z;
        if (Math.sqrt(dx * dx + dy * dy + dz * dz) < 0.72) {
          connections.push([i, j]);
        }
      }
    }

    const draw = () => {
      const cx = size / 2;
      const cy = size / 2;
      const R = size * 0.4;

      ctx.clearRect(0, 0, size, size);
      angle += 0.008;

      const cosA = Math.cos(angle);
      const sinA = Math.sin(angle);
      const cosT = Math.cos(0.4);
      const sinT = Math.sin(0.4);

      const projected = points.map((p) => {
        const rx = p.x * cosA - p.z * sinA;
        const rz = p.x * sinA + p.z * cosA;
        const ry = p.y * cosT - rz * sinT;
        const rz2 = p.y * sinT + rz * cosT;
        return {
          sx: cx + rx * R,
          sy: cy + ry * R,
          z: rz2,
        };
      });

      for (const [i, j] of connections) {
        const a = projected[i];
        const b = projected[j];
        const avgZ = (a.z + b.z) / 2;
        if (avgZ < -0.1) continue;
        const alpha = Math.max(0, (avgZ + 1) * 0.12);
        ctx.beginPath();
        ctx.moveTo(a.sx, a.sy);
        ctx.lineTo(b.sx, b.sy);
        ctx.strokeStyle = `rgba(26, 43, 74, ${alpha})`;
        ctx.lineWidth = 0.4;
        ctx.stroke();
      }

      for (const p of projected) {
        if (p.z < -0.2) continue;
        const alpha = Math.max(0.1, (p.z + 1) * 0.5);
        const s = Math.max(0.8, 1.8 * ((p.z + 1) / 2));
        const isGold = p.z > 0.4;
        ctx.beginPath();
        ctx.arc(p.sx, p.sy, s, 0, Math.PI * 2);
        ctx.fillStyle = isGold
          ? `rgba(201, 164, 76, ${alpha})`
          : `rgba(26, 43, 74, ${alpha})`;
        ctx.fill();
      }

      animId = requestAnimationFrame(draw);
    };

    draw();
    return () => cancelAnimationFrame(animId);
  }, [size]);

  return (
    <canvas
      ref={canvasRef}
      style={{ width: size, height: size, display: "block" }}
    />
  );
}
