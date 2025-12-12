

import { downloadProject } from "@/app/actions";

export async function downloadEntireProject() {
    const { file } = await downloadProject();
    return file;
}
