import Link from "next/link";

const themes = [{ id: "kafka", label: "Kafka" }];

export default function HomePage() {
  return (
    <main className="mx-auto flex min-h-screen max-w-2xl flex-col gap-6 p-8">
      <h1 className="text-3xl font-bold">glassbox</h1>
      <p className="text-gray-600">
        ミドルウェアの内部状態をリアルタイムに可視化する学習プラットフォーム
      </p>
      <ul className="flex flex-col gap-2">
        {themes.map((theme) => (
          <li key={theme.id}>
            <Link href={`/themes/${theme.id}`} className="text-blue-600 underline">
              {theme.label}
            </Link>
          </li>
        ))}
      </ul>
    </main>
  );
}
