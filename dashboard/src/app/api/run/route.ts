import { spawn } from 'child_process';
import path from 'path';
import { existsSync, copyFileSync } from 'fs';

export const dynamic = 'force-dynamic';

// Server-side mutex: prevent multiple Docker containers from spawning at once
let isRunning = false;

export async function GET() {
    const encoder = new TextEncoder();

    // Guard: reject if pipeline already running
    if (isRunning) {
        const stream = new ReadableStream({
            start(controller) {
                try {
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify('[SYSTEM] Pipeline is already running. Please wait for it to finish.')}\n\n`));
                    controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
                } catch (e) {}
                controller.close();
            }
        });
        return new Response(stream, {
            headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' }
        });
    }

    isRunning = true;

    const stream = new ReadableStream({
        start(controller) {
            const projectRoot = path.resolve(process.cwd(), '..');
            const dockerCmd = `docker run --rm --network hadoop-docker_default -v "${projectRoot}:/app" -w /app my_pyspark_edge python final_project.py`;

            const pyProcess = spawn(dockerCmd, [], { shell: true });

            const streamData = (data: Buffer) => {
                const str = data.toString();
                const lines = str.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        // Filter out noisy Spark progress bars
                        if (line.includes('[Stage ') && (line.includes('====>') || line.includes('=>'))) continue;
                        
                        try {
                            controller.enqueue(encoder.encode(`data: ${JSON.stringify(line)}\n\n`));
                        } catch (e) {}
                    }
                }
            };

            pyProcess.stdout.on('data', streamData);
            pyProcess.stderr.on('data', streamData);

            pyProcess.on('close', (code) => {
                isRunning = false;
                try {
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify(`[SYSTEM] Pipeline finished (Code: ${code})`)}\n\n`));
                } catch (e) {}

                // Copy dashboard_data.json to public/ so frontend can fetch it
                const srcJson = path.join(projectRoot, 'dashboard_data.json');
                const destJson = path.join(process.cwd(), 'public', 'dashboard_data.json');

                if (existsSync(srcJson)) {
                    try {
                        copyFileSync(srcJson, destJson);
                        try {
                            controller.enqueue(encoder.encode(`data: [CHART_READY]\n\n`));
                        } catch (e) {}
                    } catch (e) {
                        try {
                            controller.enqueue(encoder.encode(`data: ${JSON.stringify('[SYSTEM] Could not copy chart data.')}\n\n`));
                        } catch (e) {}
                    }
                }

                try {
                    controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
                } catch (e) {}
                controller.close();
            });

            pyProcess.on('error', (err) => {
                isRunning = false;
                try {
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify(`[SYSTEM ERROR] Failed to start pipeline: ${err.message}`)}\n\n`));
                } catch (e) {}
                controller.close();
            });
        }
    });

    return new Response(stream, {
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
        },
    });
}
