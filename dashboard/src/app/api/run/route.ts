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
                controller.enqueue(encoder.encode(`data: ${JSON.stringify('[SYSTEM] Pipeline is already running. Please wait for it to finish.')}\n\n`));
                controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
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
                        controller.enqueue(encoder.encode(`data: ${JSON.stringify(line)}\n\n`));
                    }
                }
            };

            pyProcess.stdout.on('data', streamData);
            pyProcess.stderr.on('data', streamData);

            pyProcess.on('close', (code) => {
                isRunning = false;
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(`[SYSTEM] Pipeline finished (Code: ${code})`)}\n\n`));

                // Copy dashboard_data.json to public/ so frontend can fetch it
                const srcJson = path.join(projectRoot, 'dashboard_data.json');
                const destJson = path.join(process.cwd(), 'public', 'dashboard_data.json');

                if (existsSync(srcJson)) {
                    try {
                        copyFileSync(srcJson, destJson);
                        controller.enqueue(encoder.encode(`data: [CHART_READY]\n\n`));
                    } catch (e) {
                        controller.enqueue(encoder.encode(`data: ${JSON.stringify('[SYSTEM] Could not copy chart data.')}\n\n`));
                    }
                }

                controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
                controller.close();
            });

            pyProcess.on('error', (err) => {
                isRunning = false;
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(`[SYSTEM ERROR] Failed to start pipeline: ${err.message}`)}\n\n`));
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
