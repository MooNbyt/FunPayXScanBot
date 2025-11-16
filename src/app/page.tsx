import FunPayWorkerDashboard from "@/components/funpay-worker-dashboard";
import { logout } from "@/app/actions";
import { Button } from "@/components/ui/button";
import { LogOut } from "lucide-react";

export default function Home() {
  return (
    <main>
      <div className="absolute top-4 right-4 z-50">
        <form action={logout}>
          <Button type="submit" variant="outline">
            <LogOut className="mr-2 h-4 w-4" />
            Выйти
          </Button>
        </form>
      </div>
      <FunPayWorkerDashboard />
    </main>
  );
}
