// 开机自启注册工具（可选，非 Termux 场景的桌面/服务器平台使用）。
//   npm run install:startup    注册开机自启（Windows 计划任务 / systemd user unit）
//   npm run uninstall:startup  移除开机自启
// Termux 环境不使用本工具，自启动由部署脚本（st_deploy.sh 菜单 12）负责。
import { execFileSync } from "node:child_process";
import { homedir, platform } from "node:os";
import { join } from "node:path";

const mode = process.argv[2];
const entry = join(process.cwd(), "dist", "server.js");

if (mode === "install") {
  if (platform() === "win32") {
    const task = `schtasks /Create /SC ONLOGON /TN "ST LLM Gateway" /TR "\\"${process.execPath}\\" \\"${entry}\\"" /F`;
    execFileSync("cmd.exe", ["/c", task], { stdio: "inherit" });
  } else {
    const dir = join(homedir(), ".config", "systemd", "user");
    execFileSync("mkdir", ["-p", dir]);
    // unit 文件必须用真实换行：写成字面 "\n"（反斜杠+n）会让文件变成单行，systemd 无法解析
    const unit = `[Unit]\nDescription=ST LLM Gateway\nAfter=network-online.target\n\n[Service]\nWorkingDirectory=${process.cwd()}\nExecStart=${process.execPath} ${entry}\nRestart=always\n\n[Install]\nWantedBy=default.target\n`;
    const { writeFileSync } = await import("node:fs");
    writeFileSync(join(dir, "st-llm-gateway.service"), unit);
    execFileSync("systemctl", ["--user","daemon-reload"]);
    execFileSync("systemctl", ["--user","enable","--now","st-llm-gateway.service"], { stdio:"inherit" });
  }
  console.log("Startup registration complete.");
} else if (mode === "uninstall") {
  if (platform() === "win32") execFileSync("schtasks", ["/Delete","/TN","ST LLM Gateway","/F"], { stdio:"inherit" });
  else execFileSync("systemctl", ["--user","disable","--now","st-llm-gateway.service"], { stdio:"inherit" });
  console.log("Startup registration removed.");
} else {
  console.log("Usage: npm run install:startup | npm run uninstall:startup");
}
