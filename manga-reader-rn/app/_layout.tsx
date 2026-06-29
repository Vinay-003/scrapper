import { useEffect, useState } from "react";
import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { View, ActivityIndicator } from "react-native";
import { loadTheme, isDark } from "../src/lib/theme";

export default function RootLayout() {
  const [ready, setReady] = useState(false);
  const [dark, setDark] = useState(true);

  useEffect(() => {
    loadTheme().then((c) => {
      setDark(isDark());
      setReady(true);
    });
  }, []);

  if (!ready) {
    return (
      <View style={{ flex: 1, justifyContent: "center", alignItems: "center", backgroundColor: "#0a0a0c" }}>
        <ActivityIndicator size="large" color="#00e5c3" />
      </View>
    );
  }

  return (
    <>
      <StatusBar style={dark ? "light" : "dark"} />
      <Stack
        screenOptions={{
          headerShown: false,
          contentStyle: { backgroundColor: dark ? "#0a0a0c" : "#f5f5f7" },
          animation: "slide_from_right",
        }}
      />
    </>
  );
}
